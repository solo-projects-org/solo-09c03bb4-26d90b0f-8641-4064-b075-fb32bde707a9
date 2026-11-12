"""FastAPI：单台 Modbus TCP 电表试采接口 + 能耗分钟明细小时留档。"""

from __future__ import annotations

import os
import sqlite3
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Body, FastAPI, HTTPException, Path, Query
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pymodbus.client import AsyncModbusTcpClient
from pymodbus.exceptions import ModbusException

from .energy_db import (
    SECONDS_PER_HOUR,
    SECONDS_PER_MINUTE,
    CutoffConflict,
    CutoffRegression,
    EnergyDB,
)
from .planner import PlanError, Segment, plan_reads

DEFAULT_HOST = os.getenv("METER_HOST", "meter")
DEFAULT_PORT = int(os.getenv("METER_PORT", "502"))
DEFAULT_SLAVE = int(os.getenv("METER_SLAVE_ID", "1"))
DEFAULT_TIMEOUT = float(os.getenv("METER_TIMEOUT", "3"))

# 单次小时查询允许的最大跨度，防止误拉全库
MAX_HOURLY_SPAN_HOURS = 24 * 366

MeterId = Annotated[str, Path(min_length=1, max_length=64, description="表号")]


# ---------------------------------------------------------------------------
# /read：Modbus 试采
# ---------------------------------------------------------------------------


class PointIn(BaseModel):
    address: int = Field(..., ge=0, description="保持寄存器起始地址（从 0 算）")
    words: int = Field(..., ge=1, le=2, description="字数：1（单字）或 2（双字，高字在前）")


class ReadRequest(BaseModel):
    points: list[PointIn] = Field(..., min_length=1, description="待试采测点，至少 1 个")
    range_start: int = Field(..., ge=0, description="设备可读区间起始地址（含两端）")
    range_end: int = Field(..., ge=0, description="设备可读区间结束地址（含两端）")
    max_read: int = Field(..., ge=1, description="单次读取寄存器字数上限")
    host: str = Field(default=DEFAULT_HOST, description="Modbus TCP 设备地址")
    port: int = Field(default=DEFAULT_PORT, ge=1, le=65535, description="Modbus TCP 端口")
    slave_id: int = Field(default=DEFAULT_SLAVE, ge=1, le=247, description="从站 / 单元号")
    timeout: float = Field(default=DEFAULT_TIMEOUT, gt=0, le=30, description="连接与读取超时（秒）")

    @model_validator(mode="after")
    def _check_range(self) -> "ReadRequest":
        if self.range_end < self.range_start:
            raise ValueError("range_end 不能小于 range_start")
        return self


class PointOut(BaseModel):
    index: int = Field(..., description="测点在请求 points 中的下标（从 0 算）")
    address: int
    words: int
    value: int = Field(..., description="无符号计数；双字时按高字在前（big-endian）拼接")


class SegmentOut(BaseModel):
    start: int = Field(..., description="读段起始地址（含）")
    end: int = Field(..., description="读段结束地址（含）")
    count: int = Field(..., description="本次读取的寄存器字数")
    point_indexes: list[int] = Field(..., description="该读段覆盖的测点下标")
    registers: list[int] = Field(..., description="读回的原始 16 位寄存器值（按地址升序）")


class ReadResponse(BaseModel):
    request_count: int = Field(..., description="实际 Modbus 请求次数（已最少化）")
    total_registers: int = Field(..., description="读取寄存器总数（次数相同时已最少化）")
    segments: list[SegmentOut]
    points: list[PointOut] = Field(..., description="按请求下标返回的测点评值")


# ---------------------------------------------------------------------------
# 能耗分钟明细 / 小时留档
# ---------------------------------------------------------------------------


class MinuteRecordIn(BaseModel):
    """一条分钟明细：分钟起始 UTC 秒 + 该分钟用电量 Wh。"""

    minute_start: int = Field(..., ge=0, description="分钟起始 UTC 秒（60 的整数倍）")
    wh: int = Field(..., ge=0, description="该分钟用电量 Wh（非负整数）")

    @field_validator("minute_start")
    @classmethod
    def _aligned_to_minute(cls, v: int) -> int:
        if v % SECONDS_PER_MINUTE != 0:
            raise ValueError("minute_start 须为整分钟起始（60 的整数倍）")
        return v


class IngestRequest(BaseModel):
    """网关按表号批量上报的分钟明细；同一分钟以最后写入的值为准。"""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "records": [
                        {"minute_start": 1767225600, "wh": 120},
                        {"minute_start": 1767225660, "wh": 118},
                        {"minute_start": 1767225720, "wh": 121},
                        {"minute_start": 1767229200, "wh": 130},
                        {"minute_start": 1767229260, "wh": 128},
                    ]
                }
            ]
        }
    )

    records: list[MinuteRecordIn] = Field(
        ..., min_length=1, max_length=10000, description="分钟明细批次（1~10000 条）"
    )


class IngestResponse(BaseModel):
    meter_id: str
    accepted: int = Field(..., description="本批写入的分钟明细条数")
    cutoff_hour: int | None = Field(..., description="该表当前清理截止点（UTC 整点秒），未清理过为 null")


class CleanupRequest(BaseModel):
    """值班员发起的单表清理：把截止点之前的明细按小时汇总留档后删除。"""

    model_config = ConfigDict(json_schema_extra={"examples": [{"cutoff_hour": 1767232800}]})

    cutoff_hour: int = Field(
        ..., ge=0, description="清理截止点（UTC 整点秒）：此前的明细按小时汇总后删除；只进不退"
    )

    @field_validator("cutoff_hour")
    @classmethod
    def _aligned_to_hour(cls, v: int) -> int:
        if v % SECONDS_PER_HOUR != 0:
            raise ValueError("cutoff_hour 须为 UTC 整点（3600 的整数倍）")
        return v


class CleanupHourOut(BaseModel):
    hour_start: int = Field(..., description="小时起始 UTC 秒（整点）")
    wh_total: int = Field(..., description="该小时 Wh 合计")
    reported_minutes: int = Field(..., description="该小时已报分钟数")


class CleanupResponse(BaseModel):
    meter_id: str
    cutoff_hour: int = Field(..., description="生效的清理截止点（UTC 整点秒）")
    aggregated_hours: int = Field(..., description="本次新留档的小时数")
    deleted_details: int = Field(..., description="本次删除的分钟明细条数")
    hours: list[CleanupHourOut] = Field(..., description="本次新留档的小时明细（按时间升序）")


class HourlyPointOut(BaseModel):
    hour_start: int = Field(..., description="小时起始 UTC 秒（整点）")
    wh_total: int | None = Field(..., description="该小时 Wh 合计；整小时无记录时为 null")
    reported_minutes: int = Field(..., description="该小时已报分钟数（缺报情况 = 60 - 该值）")


class HourlyResponse(BaseModel):
    meter_id: str
    start: int = Field(..., description="查询区间起点（UTC 整点秒，含）")
    end: int = Field(..., description="查询区间终点（UTC 整点秒，不含）")
    cutoff_hour: int | None = Field(..., description="该表当前清理截止点，未清理过为 null")
    hours: list[HourlyPointOut] = Field(..., description="区间内逐小时结果，每小时一条")


def decode_unsigned(words: list[int]) -> int:
    """无符号拼接；双字高字在前（big-endian / 高字在低地址）。"""
    value = 0
    for word in words:
        value = (value << 16) | (word & 0xFFFF)
    return value


def create_app(db_path: str | None = None) -> FastAPI:
    """组装应用；``db_path`` 缺省取环境变量 ``ENERGY_DB_PATH``（默认内存库）。"""
    energy_db = EnergyDB(db_path or os.getenv("ENERGY_DB_PATH", ":memory:"))

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        yield
        energy_db.close()

    app = FastAPI(
        title="Modbus TCP 电表试采与能耗小时留档接口",
        version="1.1.0",
        description=(
            "试采：提交测点保持寄存器起始地址（从 0 算）与字数（1 或 2），"
            "并给出设备可读区间（含两端）与单次读取上限；接口先做读段规划"
            "（请求次数最少，次数相同则读取寄存器总数最少），再逐段读取，"
            "返回读段与无符号计数（双字高字在前）。无可行方案或读取失败时"
            "直接报错，不返回部分读数。\n\n"
            "能耗留档：网关按表号批量补报分钟明细（分钟起始 UTC 秒 + 非负整数 Wh，"
            "同一分钟以最后写入为准）；值班员为单表指定 UTC 整点截止点，把此前的"
            "明细按小时汇总留档后删除（汇总、删除、截止点在 SQLite 同一事务中生效，"
            "失败则原数据不变；截止点只进不退）；截止点生效后，含更早分钟的补报"
            "批次整批拒收；按表号与 UTC 整点区间（含起点、不含终点）逐小时查询 "
            "Wh 合计与已报分钟数，整小时无记录时合计为 null，跨截止点查询也只能"
            "看到同一份已提交数据。"
        ),
        lifespan=lifespan,
    )
    app.state.energy_db = energy_db

    @app.get("/", include_in_schema=False)
    def root() -> dict[str, str]:
        return {"service": "modbus-meter-sampling", "docs": "/docs"}

    @app.post(
        "/read",
        response_model=ReadResponse,
        summary="规划读段并对单台 Modbus TCP 电表执行试采",
        responses={
            422: {"description": "请求参数校验失败 / 无可行方案"},
            502: {"description": "连接或 Modbus 读取失败（不会返回部分读数）"},
        },
    )
    async def read_meter(req: ReadRequest) -> ReadResponse:
        try:
            segments: list[Segment] = plan_reads(
                points=[(p.address, p.words) for p in req.points],
                range_start=req.range_start,
                range_end=req.range_end,
                max_read=req.max_read,
            )
        except PlanError as exc:
            # 规划阶段失败：尚未与设备通信
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        client = AsyncModbusTcpClient(
            host=req.host,
            port=req.port,
            timeout=req.timeout,
        )
        try:
            await client.connect()
            if not client.connected:
                raise HTTPException(
                    status_code=502,
                    detail=f"连接 Modbus 设备失败：{req.host}:{req.port}",
                )

            seg_out: list[SegmentOut] = []
            # point_index -> 该测点读回的原始寄存器值
            point_words: dict[int, list[int]] = {}

            for seg in segments:
                try:
                    rr = await client.read_holding_registers(
                        address=seg.start,
                        count=seg.count,
                        device_id=req.slave_id,
                    )
                except (ModbusException, OSError, TimeoutError) as exc:
                    raise HTTPException(
                        status_code=502,
                        detail=f"读取地址 {seg.start}..{seg.end} 失败：{exc}",
                    ) from exc
                if rr.isError():
                    raise HTTPException(
                        status_code=502,
                        detail=f"读取地址 {seg.start}..{seg.end} 失败：{rr}",
                    )

                registers = list(rr.registers)
                seg_out.append(
                    SegmentOut(
                        start=seg.start,
                        end=seg.end,
                        count=seg.count,
                        point_indexes=list(seg.point_indexes),
                        registers=registers,
                    )
                )
                for idx in seg.point_indexes:
                    p = req.points[idx]
                    offset = p.address - seg.start
                    point_words[idx] = registers[offset:offset + p.words]
        finally:
            client.close()

        points_out = [
            PointOut(
                index=i,
                address=p.address,
                words=p.words,
                value=decode_unsigned(point_words[i]),
            )
            for i, p in enumerate(req.points)
        ]
        return ReadResponse(
            request_count=len(segments),
            total_registers=sum(s.count for s in segments),
            segments=seg_out,
            points=points_out,
        )

    @app.post(
        "/meters/{meter_id}/minutes",
        response_model=IngestResponse,
        summary="网关按表号批量补报分钟明细（同一分钟以最后写入为准）",
        responses={
            409: {"description": "批次含早于清理截止点的分钟，整批拒收"},
            422: {"description": "请求参数校验失败（分钟未对齐 / Wh 为负等）"},
        },
    )
    async def ingest_minutes(
        meter_id: MeterId,
        req: Annotated[
            IngestRequest,
            Body(
                examples=[
                    {
                        "records": [
                            {"minute_start": 1767225600, "wh": 120},
                            {"minute_start": 1767225660, "wh": 118},
                            {"minute_start": 1767225720, "wh": 121},
                            {"minute_start": 1767229200, "wh": 130},
                            {"minute_start": 1767229260, "wh": 128},
                        ]
                    }
                ]
            ),
        ],
    ) -> IngestResponse:
        try:
            cutoff = energy_db.ingest(
                meter_id, [(r.minute_start, r.wh) for r in req.records]
            )
        except CutoffConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except sqlite3.Error as exc:
            raise HTTPException(
                status_code=500, detail=f"写入失败，本批已整体回滚：{exc}"
            ) from exc
        return IngestResponse(meter_id=meter_id, accepted=len(req.records), cutoff_hour=cutoff)

    @app.post(
        "/meters/{meter_id}/cleanup",
        response_model=CleanupResponse,
        summary="清理：截止点之前的明细按小时汇总留档后删除（事务性生效）",
        responses={
            409: {"description": "清理截止点回退（早于当前截止点）"},
            422: {"description": "cutoff_hour 不是 UTC 整点"},
        },
    )
    async def cleanup_meter(
        meter_id: MeterId,
        req: Annotated[CleanupRequest, Body(examples=[{"cutoff_hour": 1767232800}])],
    ) -> CleanupResponse:
        try:
            result = energy_db.cleanup(meter_id, req.cutoff_hour)
        except CutoffRegression as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except sqlite3.Error as exc:
            raise HTTPException(
                status_code=500, detail=f"清理失败，已整体回滚，原数据不变：{exc}"
            ) from exc
        return CleanupResponse(
            meter_id=meter_id,
            cutoff_hour=result.cutoff_hour,
            aggregated_hours=len(result.hours),
            deleted_details=result.deleted_details,
            hours=[
                CleanupHourOut(
                    hour_start=h.hour_start,
                    wh_total=h.wh_total,
                    reported_minutes=h.reported_minutes,
                )
                for h in result.hours
            ],
        )

    @app.get(
        "/meters/{meter_id}/hourly",
        response_model=HourlyResponse,
        summary="按表号与 UTC 整点区间（含起点、不含终点）逐小时查询 Wh 合计与已报分钟数",
        responses={
            422: {"description": "区间不是整点 / 终点不大于起点 / 跨度超限"},
        },
    )
    async def query_hourly(
        meter_id: MeterId,
        start: Annotated[int, Query(ge=0, description="区间起点（UTC 整点秒，含）", examples=[1767225600])],
        end: Annotated[int, Query(ge=0, description="区间终点（UTC 整点秒，不含）", examples=[1767236400])],
    ) -> HourlyResponse:
        if start % SECONDS_PER_HOUR != 0 or end % SECONDS_PER_HOUR != 0:
            raise HTTPException(
                status_code=422, detail="start/end 须为 UTC 整点（3600 的整数倍）"
            )
        if end <= start:
            raise HTTPException(status_code=422, detail="end 必须大于 start")
        if (end - start) // SECONDS_PER_HOUR > MAX_HOURLY_SPAN_HOURS:
            raise HTTPException(
                status_code=422,
                detail=f"区间过长，一次最多查询 {MAX_HOURLY_SPAN_HOURS} 小时",
            )
        try:
            cutoff, points = energy_db.query_hourly(meter_id, start, end)
        except sqlite3.Error as exc:
            raise HTTPException(status_code=500, detail=f"查询失败：{exc}") from exc
        return HourlyResponse(
            meter_id=meter_id,
            start=start,
            end=end,
            cutoff_hour=cutoff,
            hours=[
                HourlyPointOut(
                    hour_start=p.hour_start,
                    wh_total=p.wh_total,
                    reported_minutes=p.reported_minutes,
                )
                for p in points
            ],
        )

    return app


app = create_app()
