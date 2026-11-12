"""能耗分钟明细与小时留档的 SQLite 存储。

表结构：
- ``minute_detail``   网关补报的分钟明细，同一分钟以最后写入为准；
- ``hourly_archive``  清理时按小时汇总留档的 Wh 合计与已报分钟数；
- ``cleanup_cutoff``  每块表的清理截止点（UTC 整点秒），只进不退。

一致性设计：
- 清理的"按小时汇总 + 删除明细 + 推进截止点"在同一个 ``BEGIN IMMEDIATE``
  事务里提交，任一一步失败整体回滚，原数据不变；
- 单连接加锁串行化所有事务：补报与清理并发时，先提交的事务对后一个完整
  可见——先写入的到期明细必然计入汇总；后到的补报批次只要含早于截止点
  的分钟，就在自己的事务里被整批拒收；
- 小时查询在同一事务快照里合并留档与明细，跨截止点也只能看到同一份
  已提交数据。
"""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

SECONDS_PER_MINUTE = 60
SECONDS_PER_HOUR = 3600


class CutoffConflict(Exception):
    """补报批次含有早于清理截止点的分钟，整批拒收。"""


class CutoffRegression(Exception):
    """清理截止点只进不退。"""


@dataclass(frozen=True)
class ArchiveHour:
    """一个小时留档桶。"""

    hour_start: int
    wh_total: int
    reported_minutes: int


@dataclass(frozen=True)
class CleanupResult:
    """一次清理的提交结果。"""

    cutoff_hour: int
    hours: tuple[ArchiveHour, ...]  # 本次新留档的小时（按时间升序）
    deleted_details: int


@dataclass(frozen=True)
class HourlyPoint:
    """查询结果中的一小时：整小时无记录时 wh_total 为 None。"""

    hour_start: int
    wh_total: int | None
    reported_minutes: int


_SCHEMA = """
CREATE TABLE IF NOT EXISTS minute_detail (
    meter_id     TEXT    NOT NULL,
    minute_start INTEGER NOT NULL,
    wh           INTEGER NOT NULL CHECK (wh >= 0),
    PRIMARY KEY (meter_id, minute_start)
);
CREATE TABLE IF NOT EXISTS hourly_archive (
    meter_id         TEXT    NOT NULL,
    hour_start       INTEGER NOT NULL,
    wh_total         INTEGER NOT NULL CHECK (wh_total >= 0),
    reported_minutes INTEGER NOT NULL CHECK (reported_minutes BETWEEN 0 AND 60),
    PRIMARY KEY (meter_id, hour_start)
);
CREATE TABLE IF NOT EXISTS cleanup_cutoff (
    meter_id    TEXT    PRIMARY KEY,
    cutoff_hour INTEGER NOT NULL
);
"""


def _read_cutoff(conn: sqlite3.Connection, meter_id: str) -> int | None:
    row = conn.execute(
        "SELECT cutoff_hour FROM cleanup_cutoff WHERE meter_id = ?",
        (meter_id,),
    ).fetchone()
    return None if row is None else int(row[0])


class EnergyDB:
    """单表分钟明细 / 小时留档 / 清理截止点的 SQLite 存取。

    单连接 + 互斥锁串行化全部事务，供异步事件循环内直接同步调用。
    """

    def __init__(self, path: str) -> None:
        # isolation_level=None：自动提交，事务边界由显式 BEGIN/COMMIT 控制
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.execute("PRAGMA busy_timeout = 5000")
        if path != ":memory:":
            self._conn.execute("PRAGMA journal_mode = WAL")
        self._lock = threading.Lock()
        with self._locked():
            self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        with self._locked():
            self._conn.close()

    @contextmanager
    def _locked(self) -> Iterator[None]:
        with self._lock:
            yield

    @contextmanager
    def _txn(self) -> Iterator[sqlite3.Connection]:
        """一个串行化的读写事务：成功提交，任何异常整体回滚。"""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except BaseException:
                self._conn.rollback()
                raise
            else:
                self._conn.commit()

    def ingest(self, meter_id: str, records: list[tuple[int, int]]) -> int | None:
        """写入一批分钟明细（同一分钟以最后写入为准）。

        :returns: 该表当前清理截止点（无则 None）
        :raises CutoffConflict: 批次含早于截止点的分钟，整批拒收
        """
        with self._txn() as conn:
            cutoff = _read_cutoff(conn, meter_id)
            if cutoff is not None:
                too_early = [m for m, _ in records if m < cutoff]
                if too_early:
                    raise CutoffConflict(
                        f"批次含 {len(too_early)} 条早于清理截止点 {cutoff} 的分钟"
                        f"（最早 {min(too_early)}），整批拒收"
                    )
            # 按批次内顺序逐条 upsert：同一分钟以最后写入的值为准
            conn.executemany(
                """
                INSERT INTO minute_detail (meter_id, minute_start, wh)
                VALUES (?, ?, ?)
                ON CONFLICT (meter_id, minute_start) DO UPDATE SET wh = excluded.wh
                """,
                [(meter_id, m, w) for m, w in records],
            )
            return cutoff

    def cleanup(self, meter_id: str, cutoff_hour: int) -> CleanupResult:
        """把 cutoff_hour 之前的明细按小时汇总留档后删除，并推进截止点。

        汇总、删除、截止点在同一事务中生效；失败则原数据不变。

        :raises CutoffRegression: 请求的截止点早于当前截止点（不能回退）
        """
        with self._txn() as conn:
            current = _read_cutoff(conn, meter_id)
            if current is not None and cutoff_hour < current:
                raise CutoffRegression(
                    f"清理截止点不能回退：当前 {current}，请求 {cutoff_hour}"
                )
            if current is not None and cutoff_hour == current:
                # 幂等重试：截止点之后的明细本就不该被汇总，直接返回现状
                return CleanupResult(cutoff_hour=current, hours=(), deleted_details=0)

            rows = conn.execute(
                """
                SELECT minute_start - (minute_start % ?) AS hour_start,
                       SUM(wh) AS wh_total,
                       COUNT(*) AS reported_minutes
                FROM minute_detail
                WHERE meter_id = ? AND minute_start < ?
                GROUP BY hour_start
                ORDER BY hour_start
                """,
                (SECONDS_PER_HOUR, meter_id, cutoff_hour),
            ).fetchall()
            if rows:
                # 防御性合并：同一小时已有留档时累加（正常流程不会命中）
                conn.executemany(
                    """
                    INSERT INTO hourly_archive
                        (meter_id, hour_start, wh_total, reported_minutes)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT (meter_id, hour_start) DO UPDATE SET
                        wh_total = wh_total + excluded.wh_total,
                        reported_minutes = reported_minutes + excluded.reported_minutes
                    """,
                    [(meter_id, h, total, cnt) for h, total, cnt in rows],
                )
            deleted = conn.execute(
                "DELETE FROM minute_detail WHERE meter_id = ? AND minute_start < ?",
                (meter_id, cutoff_hour),
            ).rowcount
            conn.execute(
                """
                INSERT INTO cleanup_cutoff (meter_id, cutoff_hour) VALUES (?, ?)
                ON CONFLICT (meter_id) DO UPDATE SET cutoff_hour = excluded.cutoff_hour
                """,
                (meter_id, cutoff_hour),
            )
            return CleanupResult(
                cutoff_hour=cutoff_hour,
                hours=tuple(ArchiveHour(h, total, cnt) for h, total, cnt in rows),
                deleted_details=deleted,
            )

    def query_hourly(
        self, meter_id: str, start: int, end: int
    ) -> tuple[int | None, list[HourlyPoint]]:
        """按 UTC 整点区间 [start, end) 逐小时合并留档与明细。

        两个 SELECT 在同一事务快照中执行，跨截止点也只看到同一份已提交数据。
        整小时没有任何记录时 wh_total 为 None、已报分钟数为 0。
        """
        with self._txn() as conn:
            cutoff = _read_cutoff(conn, meter_id)
            archived = {
                h: (total, cnt)
                for h, total, cnt in conn.execute(
                    """
                    SELECT hour_start, wh_total, reported_minutes
                    FROM hourly_archive
                    WHERE meter_id = ? AND hour_start >= ? AND hour_start < ?
                    """,
                    (meter_id, start, end),
                )
            }
            detailed = {
                h: (total, cnt)
                for h, total, cnt in conn.execute(
                    """
                    SELECT minute_start - (minute_start % ?) AS hour_start,
                           SUM(wh), COUNT(*)
                    FROM minute_detail
                    WHERE meter_id = ? AND minute_start >= ? AND minute_start < ?
                    GROUP BY hour_start
                    """,
                    (SECONDS_PER_HOUR, meter_id, start, end),
                )
            }
        points: list[HourlyPoint] = []
        hour = start
        while hour < end:
            total = 0
            count = 0
            seen = False
            if hour in archived:
                total += archived[hour][0]
                count += archived[hour][1]
                seen = True
            if hour in detailed:
                total += detailed[hour][0]
                count += detailed[hour][1]
                seen = True
            points.append(HourlyPoint(hour, total if seen else None, count))
            hour += SECONDS_PER_HOUR
        return cutoff, points
