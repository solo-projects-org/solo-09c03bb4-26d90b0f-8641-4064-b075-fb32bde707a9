"""能耗分钟明细 / 小时留档 API 集成测试。

通过 httpx ASGI 传输调用 FastAPI，每块表使用独立的临时 SQLite 库，
不依赖 Docker 或固定端口。
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import AsyncIterator

import httpx
import pytest

from app.main import create_app

T0 = 1767225600  # 2026-01-01T00:00:00Z，UTC 整点
H = 3600


@pytest.fixture
def db_path(tmp_path) -> str:
    return str(tmp_path / "energy.db")


@pytest.fixture
def app(db_path: str):
    return create_app(db_path)


@pytest.fixture
async def client(app) -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


async def _ingest(client: httpx.AsyncClient, meter: str, records: list[dict]) -> httpx.Response:
    return await client.post(f"/meters/{meter}/minutes", json={"records": records})


async def _hourly(client: httpx.AsyncClient, meter: str, start: int, end: int) -> dict:
    resp = await client.get(f"/meters/{meter}/hourly", params={"start": start, "end": end})
    assert resp.status_code == 200
    return resp.json()


def _hours_by_start(body: dict) -> dict[int, dict]:
    return {h["hour_start"]: h for h in body["hours"]}


async def test_ingest_and_hourly_query_last_write_wins(client) -> None:
    # 同一批次内同一分钟重复：以最后写入为准
    resp = await _ingest(
        client,
        "M1",
        [
            {"minute_start": T0, "wh": 10},
            {"minute_start": T0 + 60, "wh": 20},
            {"minute_start": T0 + 120, "wh": 30},
            {"minute_start": T0, "wh": 99},  # 覆盖同批前面的 10
            {"minute_start": T0 + H, "wh": 7},
        ],
    )
    assert resp.status_code == 200
    assert resp.json()["accepted"] == 5
    assert resp.json()["cutoff_hour"] is None
    # 后一批覆盖前一批的同一分钟
    assert (await _ingest(client, "M1", [{"minute_start": T0 + 60, "wh": 55}])).status_code == 200

    body = await _hourly(client, "M1", T0, T0 + 3 * H)
    assert body["cutoff_hour"] is None
    assert [h["hour_start"] for h in body["hours"]] == [T0, T0 + H, T0 + 2 * H]
    hours = _hours_by_start(body)
    assert (hours[T0]["wh_total"], hours[T0]["reported_minutes"]) == (99 + 55 + 30, 3)
    assert (hours[T0 + H]["wh_total"], hours[T0 + H]["reported_minutes"]) == (7, 1)
    # 整小时没有记录：合计为空、已报 0 分钟
    assert hours[T0 + 2 * H]["wh_total"] is None
    assert hours[T0 + 2 * H]["reported_minutes"] == 0


async def test_ingest_and_cleanup_validation_errors(client) -> None:
    # Wh 为负
    assert (await _ingest(client, "M1", [{"minute_start": T0, "wh": -1}])).status_code == 422
    # 分钟未对齐整分钟
    assert (await _ingest(client, "M1", [{"minute_start": T0 + 30, "wh": 1}])).status_code == 422
    # 空批次
    assert (await _ingest(client, "M1", [])).status_code == 422
    # 清理截止点不是 UTC 整点
    resp = await client.post("/meters/M1/cleanup", json={"cutoff_hour": T0 + 60})
    assert resp.status_code == 422


async def test_cleanup_aggregates_deletes_and_cross_cutoff_query_consistent(client) -> None:
    records = [
        {"minute_start": T0, "wh": 10},
        {"minute_start": T0 + 60, "wh": 20},
        {"minute_start": T0 + 120, "wh": 30},
        {"minute_start": T0 + H, "wh": 40},
        {"minute_start": T0 + H + 60, "wh": 50},
        {"minute_start": T0 + 2 * H, "wh": 60},
    ]
    assert (await _ingest(client, "M1", records)).status_code == 200

    before = await _hourly(client, "M1", T0, T0 + 3 * H)

    resp = await client.post("/meters/M1/cleanup", json={"cutoff_hour": T0 + 2 * H})
    assert resp.status_code == 200
    data = resp.json()
    assert data["cutoff_hour"] == T0 + 2 * H
    assert data["aggregated_hours"] == 2
    assert data["deleted_details"] == 5
    assert data["hours"] == [
        {"hour_start": T0, "wh_total": 60, "reported_minutes": 3},
        {"hour_start": T0 + H, "wh_total": 90, "reported_minutes": 2},
    ]

    # 跨截止点查询：清理前后看到同一份已提交数据
    after = await _hourly(client, "M1", T0, T0 + 3 * H)
    assert after["hours"] == before["hours"]
    assert after["cutoff_hour"] == T0 + 2 * H

    # 明细已删：同一截止点再次清理没有可汇总内容（幂等）
    resp = await client.post("/meters/M1/cleanup", json={"cutoff_hour": T0 + 2 * H})
    assert resp.status_code == 200
    assert resp.json()["aggregated_hours"] == 0
    assert resp.json()["deleted_details"] == 0


async def test_cutoff_cannot_regress(client) -> None:
    assert (await _ingest(client, "M1", [{"minute_start": T0, "wh": 1}])).status_code == 200
    resp = await client.post("/meters/M1/cleanup", json={"cutoff_hour": T0 + 2 * H})
    assert resp.status_code == 200

    # 回退到更早的整点：拒绝
    resp = await client.post("/meters/M1/cleanup", json={"cutoff_hour": T0 + H})
    assert resp.status_code == 409
    assert "不能回退" in resp.json()["detail"]

    # 数据未受影响
    body = await _hourly(client, "M1", T0, T0 + 3 * H)
    assert _hours_by_start(body)[T0]["wh_total"] == 1
    assert body["cutoff_hour"] == T0 + 2 * H

    # 推进到更晚的整点：允许
    resp = await client.post("/meters/M1/cleanup", json={"cutoff_hour": T0 + 3 * H})
    assert resp.status_code == 200


async def test_ingest_batch_with_minutes_before_cutoff_rejected_atomically(client) -> None:
    assert (await _ingest(client, "M1", [{"minute_start": T0, "wh": 10}])).status_code == 200
    resp = await client.post("/meters/M1/cleanup", json={"cutoff_hour": T0 + H})
    assert resp.status_code == 200

    # 批次里只要含一条早于截止点的分钟，整批拒收
    resp = await _ingest(
        client,
        "M1",
        [
            {"minute_start": T0 + H - 60, "wh": 1},  # 过期
            {"minute_start": T0 + 2 * H, "wh": 2},  # 有效，但也一起拒收
        ],
    )
    assert resp.status_code == 409
    assert "整批拒收" in resp.json()["detail"]

    # 批次中的有效分钟也没有写入
    body = await _hourly(client, "M1", T0 + 2 * H, T0 + 3 * H)
    assert _hours_by_start(body)[T0 + 2 * H]["wh_total"] is None

    # 恰好等于截止点的分钟允许写入
    resp = await _ingest(client, "M1", [{"minute_start": T0 + H, "wh": 5}])
    assert resp.status_code == 200
    assert resp.json()["cutoff_hour"] == T0 + H


async def test_concurrent_backfill_and_cleanup_see_one_committed_state(client) -> None:
    # 已写入的到期明细
    assert (
        await _ingest(
            client,
            "M1",
            [{"minute_start": T0, "wh": 10}, {"minute_start": T0 + 60, "wh": 20}],
        )
    ).status_code == 200

    # 补报与清理并发：事务串行提交，先写入的到期明细必须计入汇总
    cleanup_req = client.post("/meters/M1/cleanup", json={"cutoff_hour": T0 + H})
    backfill_req = _ingest(
        client,
        "M1",
        [
            {"minute_start": T0 + 120, "wh": 99},  # 到期明细
            {"minute_start": T0 + H, "wh": 7},  # 未到期
        ],
    )
    cleanup_resp, backfill_resp = await asyncio.gather(cleanup_req, backfill_req)
    assert cleanup_resp.status_code == 200

    if backfill_resp.status_code == 200:
        # 补报先提交：到期明细计入汇总，未到期分钟留作明细
        assert cleanup_resp.json()["deleted_details"] == 3
        expected_h0 = (10 + 20 + 99, 3)
        expected_h1 = (7, 1)
    else:
        # 清理先生效：含到期分钟的补报整批拒收
        assert backfill_resp.status_code == 409
        assert cleanup_resp.json()["deleted_details"] == 2
        expected_h0 = (10 + 20, 2)
        expected_h1 = (None, 0)

    # 无论哪种时序，跨截止点查询都只能看到同一份已提交数据
    body = await _hourly(client, "M1", T0, T0 + 2 * H)
    hours = _hours_by_start(body)
    assert (hours[T0]["wh_total"], hours[T0]["reported_minutes"]) == expected_h0
    assert (hours[T0 + H]["wh_total"], hours[T0 + H]["reported_minutes"]) == expected_h1


async def test_cleanup_failure_rolls_back_everything(client, db_path: str) -> None:
    records = [
        {"minute_start": T0, "wh": 10},
        {"minute_start": T0 + 60, "wh": 20},
        {"minute_start": T0 + H, "wh": 30},
    ]
    assert (await _ingest(client, "M1", records)).status_code == 200
    before = await _hourly(client, "M1", T0, T0 + 2 * H)

    # 用触发器让留档 INSERT 在清理事务中段失败
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TRIGGER fail_archive_insert BEFORE INSERT ON hourly_archive "
        f"WHEN NEW.hour_start = {T0} "
        "BEGIN SELECT RAISE(FAIL, 'injected failure'); END;"
    )
    conn.close()

    resp = await client.post("/meters/M1/cleanup", json={"cutoff_hour": T0 + 2 * H})
    assert resp.status_code == 500

    # 原数据不变：明细仍在、未留档、截止点未推进（过期分钟仍可补报）
    after = await _hourly(client, "M1", T0, T0 + 2 * H)
    assert after["hours"] == before["hours"]
    assert after["cutoff_hour"] is None
    assert (await _ingest(client, "M1", [{"minute_start": T0 + 120, "wh": 5}])).status_code == 200

    # 去掉故障后清理可重试成功，且把补报的到期分钟一并汇总
    conn = sqlite3.connect(db_path)
    conn.execute("DROP TRIGGER fail_archive_insert")
    conn.close()
    resp = await client.post("/meters/M1/cleanup", json={"cutoff_hour": T0 + 2 * H})
    assert resp.status_code == 200
    assert resp.json()["deleted_details"] == 4
    assert resp.json()["hours"][0] == {
        "hour_start": T0,
        "wh_total": 10 + 20 + 5,
        "reported_minutes": 3,
    }


async def test_hourly_query_range_validation_and_empty_meter(client) -> None:
    # 未知表：逐小时为空
    body = await _hourly(client, "NOPE", T0, T0 + 2 * H)
    assert body["cutoff_hour"] is None
    assert [h["wh_total"] for h in body["hours"]] == [None, None]
    assert [h["reported_minutes"] for h in body["hours"]] == [0, 0]

    # 非整点
    resp = await client.get("/meters/M1/hourly", params={"start": T0 + 60, "end": T0 + H})
    assert resp.status_code == 422
    # 终点不大于起点
    resp = await client.get("/meters/M1/hourly", params={"start": T0 + H, "end": T0 + H})
    assert resp.status_code == 422
    # 跨度超限
    resp = await client.get(
        "/meters/M1/hourly", params={"start": T0, "end": T0 + (24 * 366 + 1) * H}
    )
    assert resp.status_code == 422
