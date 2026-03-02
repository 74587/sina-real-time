# sina-real-time

新浪财经 WebSocket 实时行情采集工具（Rust 实现）

## 功能

- 连接 `wss://hq.sinajs.cn/wskt` 实时推送接口
- 自动重连（指数退避，最大 30 秒间隔）
- 按 `--chunk-size` 拆分为多个并行 WebSocket 连接，支持全量沪深 A 股（5189 只）
- 数据持久化到本地 CSV，按天轮转（`data_YYYY-MM-DD.csv`）
- 基于 Tokio 异步运行时，内置缓冲通道解耦采集与存储

## 实测性能（10 核 / 16 GB / macOS）

| 股票数 | 连接数 | 写入速度 |
|--------|--------|----------|
| 100    | 1      | ~45 条/秒 |
| 1000   | 2      | ~440 条/秒 |
| 5189   | 11     | ~968 条/秒 |

## 股票列表文件

```
sina-collector/
├── stocks.txt           # 12 只样本（测试用）
├── stocks_100.txt       # 100 只（用 fetch_stocks.py 生成）
├── stocks_1000.txt      # 1000 只
└── stocks_all.txt       # 5189 只全量沪深 A 股（已预生成）
```

## 快速开始

```bash
cd sina-collector
cargo build --release

# 样本测试（12 只）
./target/release/sina-collector

# 100 只
./target/release/sina-collector --stocks stocks_100.txt --output data

# 1000 只
./target/release/sina-collector --stocks stocks_1000.txt --output data

# 全量沪深 A 股（5189 只，11 个并行连接）
./target/release/sina-collector --stocks stocks_all.txt --output data
```

## 更新股票列表

```bash
# 重新从新浪 API 拉取最新股票列表（约 53 次请求，耗时 ~3 秒）
python3 scripts/fetch_stocks.py

# 同时生成 100 只样本
python3 scripts/fetch_stocks.py --sample 100

# 指定输出路径
python3 scripts/fetch_stocks.py -o /path/to/stocks.txt
```

## 数据格式

CSV 文件每行：
```
received_at,code,fields
2026-02-26T14:33:46.037,sh600519,"贵州茅台,昨收,今开,当前,最高,最低,买1,卖1,成交量,成交额,<买5档>,<卖5档>,日期,时间,状态"
```

`fields` 列字段顺序：
`名称, 昨收, 今开, 当前价, 最高, 最低, 买一价, 卖一价, 成交量(股), 成交额(元), 买5档×(量,价)×5, 卖5档×(量,价)×5, 日期, 时间, 状态`

## 命令行参数

```
Options:
  -s, --stocks <FILE>        股票列表文件 [default: stocks.txt]
  -o, --output <DIR>         输出目录 [default: data]
      --chunk-size <N>       每个连接的股票数 [default: 500]
      --buffer <N>           通道缓冲容量 [default: 131072]
  -h, --help
  -V, --version
```

## 日志级别

```bash
RUST_LOG=debug ./target/release/sina-collector --stocks stocks_all.txt --output data --chunk-size 200  # 详细日志
RUST_LOG=warn  ./target/release/sina-collector   # 仅告警
```

## 后续扩展

`storage.rs` 中的 `mpsc::Receiver<String>` 可替换为 `broadcast` channel，
向下游实时分析模块（Python、数据库写入器等）同时分发数据，无需修改采集层。
