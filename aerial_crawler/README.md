# 航拍底片扫描件采集项目

把地方测绘档案馆旧航拍底片的模拟站点内容采集到本地，供编目员查阅。
**纯 Python 标准库实现，无第三方依赖**，包含一个自带的、确定性可回放的 HTTP 模拟站。

## 目录结构

```
aerial_crawler/
├── aerial/                # 采集端
│   ├── config.py          # 运行配置（间隔、并发、退避、补登窗口等）
│   ├── limiter.py         # 全站共享的最小发起间隔限流器 + 统一暂停
│   ├── http.py            # 退避重试 / 503 统一暂停 / Range 分段的 HTTP 客户端
│   ├── parsing.py         # 分页目录与卷宗详情 HTML 解析
│   ├── state.py           # 工作目录状态（水位、失败队列、单实例锁、原子写）
│   ├── manifest.py        # JSON Lines 事件清单 + 每卷当前条目原子视图
│   ├── diagnose.py        # 续采诊断：分段请求诊断流 + 只读诊断报告（不取写锁）
│   ├── downloader.py      # 分段下载、断点区段复用、sha256 校验装配
│   └── scanner.py         # 全量 / 增量采集调度、worker 池、补齐轮次
├── mocksite/              # 可回放的 HTTP 模拟站
│   ├── scenario.py        # 固定场景与 20 倍速虚拟站点时钟
│   └── server.py          # 分页目录 / 详情 / Range 影像 / 故障注入 / 限流
├── tests/test_rules.py    # 规则级单元测试（unittest）
├── tests/test_diagnose.py # 续采诊断单元测试（强杀未知、换版归属、复用回溯）
├── verify.py              # 一条命令的端到端验证（80 秒内出可读报告）
└── README.md
```

## 一、启动模拟站

```bash
python3 -m mocksite --host 127.0.0.1 --port 8080
# 可选 --rate 20：虚拟站点时钟相对真实时间的倍速（默认 20）
```

- 所有响应带头 `X-Site-Time`（虚拟站点秒，基准 `1700000000`），一切更新时间 /
  补登判定均以**站点时间**为准；
- 路由：
  - `GET /catalog?page=N` 分页 HTML 目录（按更新时间倒序，含重复链接）；
  - `GET /volumes/<卷宗号>` 卷宗详情：卷宗编号、按秒版本更新时间、sha256 校验值、大小；
  - `GET /images/<卷宗号>` 影像，支持 `Range` 分段读取，**换版后下载地址沿用**；
  - `GET /healthz` 健康检查（不限流）。
- 内置场景：
  | 卷宗 | 场景 |
  |---|---|
  | V0003 | 目录中重复出现链接 |
  | V0002 | 影像头两次请求 500，验证退避重试 |
  | V0005 | 影像首次请求 503 + `Retry-After`，验证统一暂停 |
  | V0006 | 换版：v2 更新时间严格递增，下载地址沿用 |
  | V0007 | 补登：更新时间后 120 站点秒才上目录（10 分钟窗口内），影像再晚 60 秒开放 |
  | V0008 | 大卷宗，用于中断恢复 |
- 服务端另有 1 秒滚动窗口限流（默认 14 请求/秒），超出返回 503 + `Retry-After`。

## 二、全量采集

```bash
python3 -m aerial \
  --base-url http://127.0.0.1:8080 \
  --workdir ./data \
  --workers 4 --min-interval 0.05 \
  full
```

全量会翻完整个目录，对每卷拉详情、分段下载影像、校验后入库。
常用参数：

| 参数 | 含义 | 默认 |
|---|---|---|
| `--workers` | 并发 worker 数（共用一个限流器） | 4 |
| `--min-interval` | 全站统一的最小请求发起间隔（秒） | 0.05 |
| `--chunk-size` | 分段下载区段大小（字节） | 65536 |
| `--retry-rounds` | 主扫描后的补齐轮数 | 4 |
| `--retry-round-interval` | 补齐轮之间等待秒数 | 1.0 |
| `--max-retries` | 单请求最大重试次数 | 6 |

## 三、增量采集

```bash
python3 -m aerial --base-url http://127.0.0.1:8080 --workdir ./data incremental
```

使用**同一工作目录**。增量只查两类范围（以站点时间为准）：

1. **上次扫描以来的更新**：已知卷宗中 `updated_at` 大于上次扫描水位的条目（换版）；
2. **仍可能补登的范围**：站点当前时间减 10 分钟（`--backfill-seconds`，默认 600）
   地平线之内的未知卷宗；地平线之外的旧条目不查。

水位取“**本次扫描启动那一刻**”的站点时间：扫描期间才生效、但本次未采到的更新，
下一次增量仍会被复查，不会被水位永久跳过。

## 四、工作目录与产物

```
data/
├── state/
│   ├── state.json      # 模式、增量水位、已知卷宗（原子写）
│   ├── failed.json     # 待补齐卷宗及尝试次数（原子写，清空时可能不存在）
│   └── lock            # 单实例文件锁
├── manifest/
│   ├── events.jsonl        # 只追加的事件流，采集期间编目员可随时逐行读取
│   ├── current.json        # 每卷唯一一个当前条目（原子替换）
│   └── diagnostics.jsonl   # 只追加的续采诊断流（每次逻辑请求 start/done，每行 fsync）
├── images/<卷宗号>/
│   ├── v<n>_<校验前缀>.img   # 校验通过的完整影像（不可变，旧版保留）
│   └── chunks/...            # 下载中的持久化区段（装配后清理）
└── logs/run.log
```

关键保证：

- **每卷只有一个当前条目**，始终指向校验通过的完整影像；
- **替换件校验通过前旧件继续可查**，`current.json` 用临时文件 + 原子替换切换；
- **较晚完成的旧版下载不会让清单回退**：提交时按 `updated_at` 单调保护，
  旧版完成只记 `superseded` 事件；
- **同一卷同一版本的重复链接只拉一份完整影像**（目录去重 + 已校验影像复用）；
- **进程被 `kill -9` 后用原工作目录续采**：已 fsync 的区段按长度校验后直接复用，
  只补缺档；状态/清单文件均为原子写，不会出现半截 JSON；
- 下载暂时失败的卷宗进入持久化失败队列，主扫描与其他卷宗继续，
  随后由补齐轮次重试，并完整重翻目录捕获迟到的补登条目。

退出码：`0` 全部完成；`2` 仍有卷宗待补齐（状态已落盘，再跑一次即可续）；
`73` 工作目录被其他进程占用。

## 五、续采诊断（值班员只读查询，持锁可查）

```bash
python3 -m aerial --workdir ./data diagnose                 # 全量文本报告
python3 -m aerial --workdir ./data diagnose --volume V0008  # 只看单卷
python3 -m aerial --workdir ./data diagnose --volume V0006 --version 1
python3 -m aerial --workdir ./data diagnose --run 2 --json  # 机器可读
```

诊断命令**不发起任何网络请求、不获取写锁、不创建目录**，采集进程持有
`flock` 写锁期间也能并行查询。数据来源仅限三份清单产物：

- `manifest/current.json`、`events.jsonl`、`diagnostics.jsonl`；
- 对诊断流中**登记过的**区段/影像路径做一次 `stat` 取元数据——
  **不打开影像内容、不 rglob 扫描 images 目录、不读 logs/run.log**，
  因此单卷查询的开销与全量卷宗数无关。

`diagnostics.jsonl` 在采集侧逐行 fsync 追加，记录四类逻辑请求
（`catalog` / `detail` / `probe` / `chunk`）的 `start` 与 `done`，
`done` 含底层 HTTP 尝试明细（被重试的 503/500 也留痕）。报告口径：

| 维度 | 取值与含义 |
|---|---|
| 每次分段请求 | 逐行区分 `served`（实际发请求且落盘）、`reused`（未发请求，复用本地区段）、`failed`、`unknown` |
| 强杀在途 | 有 `start` 无 `done` 一律标 **未知**，绝不臆测；再按登记路径 stat 给出“区段实际存在 / 无落盘证据”旁证 |
| 落盘核对 | `chunk_present`（区段在盘且长度相符）、`assembled_committed`（已装配提交、区段目录已清理但完整影像在）、`chunk_missing`、`unexpected_present`（失败登记却有有效区段） |
| 首次发布轮次 | 取自事件流中该版本首个 `download_succeeded`/`replaced` 事件的 `run_seq + round`（主扫描=0，补齐轮=1..N），复用与换版都不会覆盖 |
| 版本归属 | `current`（当前条目）/ `superseded`（曾发布后被更新版替换，旧版影像保留）/ `unpublished`（从未进入当前清单） |
| 运行状态 | `running`（进程存活且持锁）/ `finished`（有 run_end）/ `crashed`（无 run_end 且进程已退出，疑似强杀） |
| 一致性核对 | 事件流提交记录与 current.json 的版本、校验值、影像路径逐项比对，不一致时退出码为 `4` |

**强杀后的状态保证**：未获确证的请求永久保留“未知”结论，续采重拉只新增
新运行序号的记录行，不会改写历史；同址换版（下载地址沿用）或整卷复用时，
旧版的归属与首次发布轮次仍可在报告中按版本分行查到。

## 六、一条命令完成端到端验证（80 秒内）

```bash
python3 verify.py
```

自动找空闲端口、起模拟站、建临时目录，依次覆盖**补登、限流、换版、中断恢复**，
最后打印逐条 PASS/FAIL 报告与总用时（实测约 47 秒，稳定可回放）。
需要保留现场时：

```bash
python3 verify.py --workdir ./verify_data --keep
```

验证脚本内部会：全量下载中 `SIGKILL` 强杀 → 强杀后立即只读诊断并断言
“未知请求 + 落盘旁证” → 同目录续采并断言“复用 N 段” → 持锁并行诊断 →
等到补登条目与换版生效后跑增量 → 对所有当前影像重新计算 sha256 比对，
并核对同址换版后旧版归属与首次发布轮次仍可查。

## 七、运行单元测试

```bash
python3 -m unittest discover -s tests -v
```
