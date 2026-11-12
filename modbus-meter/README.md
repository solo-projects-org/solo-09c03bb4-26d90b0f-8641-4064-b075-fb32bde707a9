# Modbus TCP 电表试采与能耗小时留档接口

两部分功能：

1. **试采**：新电表接入能耗库前的单台 Modbus TCP 电表试采服务。厂务工程师提交
   测点的保持寄存器起始地址（从 0 算）与字数（1 或 2），并给出设备的可读区间
   （含两端）与单次读取上限；接口自动规划读段、按方案读表，返回读段与无符号
   计数（双字按高字在前拼接）。
2. **能耗小时留档**：网关按表号批量补报分钟明细；过了补报期限后，厂务数仓
   值班员为单表指定 UTC 整点截止点，把此前的明细按小时汇总留档后删除（明细
   清了，能耗库仍能查到每小时的用电量和缺报情况）。

## 快速开始（Docker Compose）

```bash
docker compose up --build
```

- API：<http://localhost:8000>，Swagger UI：<http://localhost:8000/docs>
- 模拟电表：`localhost:1502`（容器内 `meter:502`），保持寄存器 0..99
- 能耗库：SQLite 文件 `/data/energy.db`，挂在命名卷 `energy-data` 上，
  容器重建不丢留档

在 Swagger 中打开 `POST /read` → Try it out → 直接 Execute 即可对模拟表试采
（请求体默认指向容器内的模拟表）。也可在宿主机用 curl：

```bash
curl -X POST http://localhost:8000/read \
  -H 'Content-Type: application/json' \
  -d '{
    "points": [
      {"address": 0, "words": 2},
      {"address": 2, "words": 2},
      {"address": 10, "words": 1},
      {"address": 12, "words": 1}
    ],
    "range_start": 0,
    "range_end": 99,
    "max_read": 8
  }'
```

## 能耗小时留档 API

时间一律用 UTC 秒：分钟明细的 `minute_start` 须为 60 的整数倍，截止点与查询
区间须为 3600 的整数倍（UTC 整点）。下面样例中
`1767225600` = 2026-01-01T00:00:00Z。

### `POST /meters/{meter_id}/minutes` —— 网关批量补报分钟明细

每条含分钟起始 UTC 秒和非负整数 Wh；同一分钟以最后写入的值为准（同批内
重复或跨批覆盖都生效）。

```bash
curl -X POST http://localhost:8000/meters/MTR-001/minutes \
  -H 'Content-Type: application/json' \
  -d '{
    "records": [
      {"minute_start": 1767225600, "wh": 120},
      {"minute_start": 1767225660, "wh": 118},
      {"minute_start": 1767225720, "wh": 121},
      {"minute_start": 1767229200, "wh": 130},
      {"minute_start": 1767229260, "wh": 128}
    ]
  }'
# {"meter_id":"MTR-001","accepted":5,"cutoff_hour":null}
```

截止点生效后，批次只要含一条更早的分钟就**整批拒收**（`409`，批次中的有效
分钟也不会写入）；恰好等于截止点的分钟允许写入。

### `POST /meters/{meter_id}/cleanup` —— 清理：按小时留档后删除明细

值班员为单表指定 UTC 整点截止点，把此前的明细按小时汇总（Wh 合计 + 已报
分钟数）留档后删除。汇总、删除和截止点推进在 SQLite 同一事务中生效，失败
则原数据不变；截止点只进不退（回退返回 `409`，同一截止点重试幂等）。

```bash
curl -X POST http://localhost:8000/meters/MTR-001/cleanup \
  -H 'Content-Type: application/json' \
  -d '{"cutoff_hour": 1767232800}'   # 2026-01-01T02:00:00Z
# {"meter_id":"MTR-001","cutoff_hour":1767232800,"aggregated_hours":2,
#  "deleted_details":5,"hours":[
#    {"hour_start":1767225600,"wh_total":359,"reported_minutes":3},
#    {"hour_start":1767229200,"wh_total":258,"reported_minutes":2}]}
```

### `GET /meters/{meter_id}/hourly` —— 按 UTC 整点区间逐小时查询

含起点、不含终点；逐小时返回 Wh 合计和已报分钟数（缺报情况 = 60 − 已报
分钟数），整小时没有记录时合计为 `null`。留档与明细在同一事务快照中合并，
跨截止点查询也只能看到同一份已提交数据——清理前后查同一区间结果一致。

```bash
curl 'http://localhost:8000/meters/MTR-001/hourly?start=1767225600&end=1767236400'
# {"meter_id":"MTR-001","start":1767225600,"end":1767236400,
#  "cutoff_hour":1767232800,"hours":[
#    {"hour_start":1767225600,"wh_total":359,"reported_minutes":3},
#    {"hour_start":1767229200,"wh_total":258,"reported_minutes":2},
#    {"hour_start":1767232800,"wh_total":null,"reported_minutes":0}]}
```

错误码：`422` 参数校验失败（未对齐整分钟/整点、Wh 为负、区间终点不大于
起点、跨度超过 8784 小时等）；`409` 截止点回退或补报含过期分钟；
`500` 存储层失败（已回滚，原数据不变）。

## 接口说明：`POST /read`

请求体：

| 字段 | 说明 |
| --- | --- |
| `points` | 测点列表，`address` 为保持寄存器起始地址（从 0 算），`words` 为字数（1 或 2） |
| `range_start` / `range_end` | 设备可读区间，含两端 |
| `max_read` | 单次读取寄存器字数上限 |
| `host` / `port` / `slave_id` / `timeout` | 设备连接参数，默认指向 Compose 中的模拟表 |

读段规划规则：

1. 每次读取一段连续寄存器，读段整体落在可读区间内，字数不超过 `max_read`；
2. 每个测点必须被某一次读取完整覆盖（不允许跨段拼接）；
3. 先使请求次数最少；次数相同时使读取寄存器总数最少
   （按地址排序后动态规划求解，读段紧贴测点，不多读无用寄存器）。

响应（200）：

- `request_count` / `total_registers`：实际请求次数与读取寄存器总数；
- `segments`：各读段的起止地址（含）、字数、覆盖的测点下标、原始寄存器值；
- `points`：按请求下标返回的测点值，`value` 为无符号计数，
  双字按高字在前（big-endian）拼接。

错误：

- `422`：参数校验失败或无可行方案（测点超出可读区间、字数超过单次上限等），
  `detail` 为中文原因；
- `502`：连接失败或 Modbus 读取失败（含设备异常响应），**不返回部分读数**。

## 模拟电表数据

`simulator/meter_sim.py`：保持寄存器 0..99，可读区间 `[0, 99]`。

| 地址 | 值 | 说明 |
| --- | --- | --- |
| 0..1 | 0x0001, 0x86A0 | 32 位无符号 100000（高字在前） |
| 2..3 | 0x0000, 0x0064 | 32 位无符号 100 |
| 10..13 | 1234, 5678, 65535, 42 | 单字演示值 |
| 其余 | `(addr * 37 + 11) & 0xFFFF` | 确定性填充，便于人工核对 |

## 本地开发（无 Docker）

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
METER_PORT=1502 .venv/bin/python -m simulator.meter_sim &
ENERGY_DB_PATH=./energy.db METER_HOST=127.0.0.1 METER_PORT=1502 \
  .venv/bin/uvicorn app.main:app --port 8000
.venv/bin/pytest            # 21 个用例：规划算法 + 试采集成 + 小时留档
```

环境变量：`METER_HOST` / `METER_PORT` / `METER_SLAVE_ID` / `METER_TIMEOUT`
为试采请求体连接参数的默认值；`ENERGY_DB_PATH` 为能耗 SQLite 库路径
（Compose 中为 `/data/energy.db`，缺省为内存库，重启即清空）。

## 目录结构

```
app/main.py        FastAPI 接口：/read 试采 + 分钟明细/清理/小时查询
app/planner.py     读段规划（动态规划：次数最少 -> 寄存器总数最少）
app/energy_db.py   能耗 SQLite 存储：分钟明细、小时留档、清理截止点（事务）
simulator/         pymodbus 内存模拟电表
tests/             规划单元测试 + API 集成测试（内存模拟表/临时库，无需 Docker）
docker-compose.yml api + meter 两个服务，energy-data 卷持久化能耗库
```
