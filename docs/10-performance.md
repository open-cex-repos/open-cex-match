# 性能基线与压测工具链

> **本章解决**：引擎能跑多快、瓶颈在哪、如何调优。
> **前置依赖**：01–03（核心机制）、05（录制是上限瓶颈）。
> **读者前置**：吞吐 / 延迟 / JFR。

## 1. 性能基线

### 1.1 全链路标定（120 回环，单进程池子注入，五进程 + Archive）

| 目标 TPS | 注入结果 | 拒绝/积压/过载 | 结论 |
| --- | --- | --- | --- |
| 600K | 600,005 | 0/0/0 | 精确打满 |
| 700K | 700,005 | 0/0/0 | 精确打满 |
| 1M | 1,000,000 | 0/0/0 | 精确打满 |
| 1.2M | 1,200,005 | 0/0/0 | 精确打满 |
| 1.5M | 1,500,000 | 0/0/0 | 精确打满 |
| 2M | 2,000,000 | 0/0/0 | 精确打满（1.9 亿帧/95s） |
| 2.5M | 2,183,402 | 0/0/0 | 受 WAL 2GiB 卡顿拖累（修复后可打满） |
| 3M | 3,000,010 | 0/0/0 | **精确打满**（短周期 WAL 修复后，95s 长测） |
| 3M×600s (10M 簿) | 3,000,012 | 1.42 亿拒绝（容量保护）/0/0 | **600s 长测稳定**：10M 订单簿 + 4G 堆，堆峰值 1.95GB，GC 3 次 young/0 full |
| 4M×90s (15M 簿) | 4,000,000 | 0/0/0 | **精确打满**（WAL 批量二进制后） |
| 4M×600s (15M 簿) | 3,999,991 | 0/0/0 | 600s 长测稳定，5.38 亿成交，1 次 full GC 27ms（343s 堆撞 4GB） |
| 5M×90s (15M 簿, 6G) | 4,183,157 ~ 4,360,131 | 0/0/0 | **链路多点饱和上限 ≈ 4.3M ± 3%**：ingress 透传 + WAL 批量 + egress 编码优化后 4.36M |

对比 Wi-Fi 时代 164K：架构链路改造 + 回环 + 单进程池子注入提升约 **13 倍**。

3M 之上：4M 目标在 15M 订单簿 + 4G 堆下可**精确打满**（600s 稳定）；5M 目标触达
链路多点饱和——core 撮合线程（cancel 20%/orderIndex 哈希 9%/findUpdate 5%）+ egress
WAL 写入（Chronicle 底层）并行封顶 ≈ 4.3M。10M 订单簿长测验证纵表内存效率：
4G 堆峰值 1.95GB（一半），GC 3 次 young / 0 full——订单簿容量不是这台机器的内存墙。

### 1.2 热点分布（5M JFR，2026-08-28 三进程）

- **core（~54% 总采样）**：撮合线程独占；`OrderBook.cancel` ~14-20% +
  `Long2IntMap.get`（orderIndex 撤单定位）~9-18% + `LevelSlots.findUpdate` ~5%——
  70% 撤单率下撤单路径是大头；2026-08-29 槽位内嵌 orderId 后 `orderIndex` 哈希
  已整体消除（02-data-structure.md §1.3），此热点归零，待复标定；
- **egress（~15% 以下）**：WAL 持久化移出后仅剩 `OutboundSender`（SBE 批量编码）
  + 深度发布；原 wal-write 线程（Chronicle 底层 ~33% 硬成本）已整体删除；
- **ingress（~10-15%）**：`AeronOrderInRunner.run`（Aeron poll 帧遍历）；
  HashMap 路由/编解码已消除（透传 + int 索引）。

**结论（改造后待复标定）**：4M 基线打满不受影响；5M 上限此前由 core 撮合线程 +
egress WAL 写入（Chronicle）并行饱和决定，WAL 移出 egress 后瓶颈应只剩 core
撮合线程本身（撤单路径 + 匹配），上限预期可上探。

### 1.3 Chronicle 2GiB/4GiB 墙（历史根因，2026-08-28 已随 WAL 架构更换根治）

3M 两次复现"整链冻结"（producer 卡死、三 JVM 采样空洞、CPU 骤降），早期误判为
线程调度饿死；**经独立探针 + 实时 jcmd + 反编译定位，真因是 Chronicle Queue
单文件边界卡死**：

1. **触发**：WAL 单文件跨 2GiB/4GiB/6GiB… 边界时，`StoreAppender.resetPosition →
   lastSequenceNumber → linearScanByPosition0` 触发**全文件线性扫描**
   （逐条读记录头 + skip，SCQIndexing.java:649），耗时随文件大小/写速增长
   （100MB/s 下跨 4GiB 卡 10~16s，850MB/s 下卡 ~24s）；
2. **级联**：WAL 线程卡 → egress 停消费 → Aeron 背压逐级传导 core→ingress→
   producer → 整链阻塞（CPU 低、看似机器冻结；外部 ticker 证明机器正常）；
3. **击穿**：卡顿 >10s 时 egress conductor 看门狗
   （`ConductorServiceTimeoutException`，timeout=10s）FATAL 终止客户端；
4. **临时修复（已废弃）**：`WalRollCycle` 自定义秒级滚动（`match.wal.roll-seconds`），
   单文件 <2GiB 从根上绕开；3M 90s 长测 3,000,010 TPS 稳定达成；
5. **根治（2026-08-28 续）**：WAL 持久化整体迁移到 Aeron Archive recording——
   Archive 按 `segmentFileLength`（默认 1GiB，2 的幂）分片，位置跨段连续映射，
   无"单文件线性扫描"路径；`WalWriter`/`WalRollCycle`/滚动配置/单写者守卫
   （`-Ddisable.single.threaded.check=true`）全部删除。

连带暴露：producer offer 无超时 + 脚本 `wait` 无限等 → run 挂死（后续加看门狗）。

### 1.4 突破方向

已落地（2026-08-28）：WAL 批量裸二进制（egress CPU 减半）、出站编码免
ThreadLocal + symbol 预编码、ingress 透传 + int 索引路由、价格档命中加速尝试
（见 §3 教训）。
已落地（2026-08-29 曾、2026-09-01 续 回退）：槽位内嵌 orderId 方案因"ID 即
存储地址"耦合（槽位回绕冲突税 + 发号方无法自由分配）已回退为 orderIndex
（Long2IntMap）映射 + freelist（02-data-structure.md §1.3）；热路径代价 = 一次 O(1) 哈希，吞吐待复测。

剩余候选：

1. ~~**core 撤单路径**~~（2026-08-29 完成、2026-09-01 续 回退）：orderIndex.get
   哈希曾由槽位内嵌 orderId 消除；回退后撤单恢复一次 O(1) 哈希（吞吐待复测）；
2. ~~egress Chronicle 底层~~（已完成）：WAL 迁到 Aeron Archive recording 后，
   egress 不再有 WAL 写线程，原 ~33% 硬成本整体消除（复标定 5M 上限后确认）；
3. **多 symbol 分片**：每 symbol 一个撮合线程，2 个 symbol 线性翻倍（架构级突破）。

### 1.5 多 producer / 裸核对照（2026-08-28）

验证两件事：Aeron 多 producer 语义、以及去掉 ingress 后 match-core 的真实上限。
`stress-test.sh --core-only` 仍带 ingress（producer → UDP → ingress → IPC 环）；
`--bare-core` 让 producer 直连入站 IPC 环（264B 入站行 `[8B 时刻][101B 帧][零填充]`，
共享媒体驱动目录），彻底跳过 ingress 进程：

| 场景 | 总 TPS | 发送失败 | 背压退避 | Match-Handler 空闲 |
| --- | --- | --- | --- | --- |
| core-only + ingress，单 producer（目标 7M） | 4,319,698 | 0 | 48,475 | 0.0% |
| core-only + ingress，双 producer（目标 3.5M×2） | 2,898,880（1,605,555 + 1,293,325） | 0 | 各 ~5.2 万 | 0.0% |
| **bare-core，单 producer（目标 7M）** | **4,948,823** | 0 | 56,586 | 0.0% |
| bare-core，双 producer（目标 3.5M×2） | 4,599,991（3,061,730 + 1,538,261） | 0 | 各 ~5.9 万 | 0.0% |

（同参数：目标 7M、60s、池子 35M 帧、堆 6g、70% 撤单率；产物
`/tmp/perf-barecore-7m`、`/tmp/perf-barecore-7m-2p`。）

**Aeron 多 producer 语义（官方文档）**：同一 channel+stream 上的多个
`ExclusivePublication` 各自独立 sessionId，订阅方以 Image 聚合、可用
`header.sessionId()` 区分；排序仅在单 Image 内保证，跨 session 无总序（订单入站无碍）。
参见 [Channels, Streams & Sessions](https://aeron.io/docs/aeron/aeron-channel-stream-session/)；
媒体驱动默认 DEDICATED 线程模型，Conductor/Sender/Receiver 各一线程，Receiver 单线程
poll 所有 UDP socket（[Aeron performance cookbook](https://aeron.io/docs/cookbook-content/aeron-increase-performance/)）。

**归因修正（§1.1 链路多点饱和）**：

1. **去掉 ingress 收益 +15%**：4.32M → 4.95M。ingress 的 `aeron-order-in` 单消费线程
   （`sub.poll(handler, 256)` 合并全部 image）确是一跳固定成本；
2. **多 producer 无解**：四种形态里 2 producer 的合计都低于对应 1 producer
   （带 ingress 2.90M < 4.32M；裸跑 4.60M < 4.95M）。多 producer 只增加每消息固定开销
   （多 image 轮询、多 session 协议/流控帧、批处理连续性被打断、进程间争用）；
3. **撮合线程就是墙**：四跑 JFR 的 `Match-Handler` 线程全部 **0.0% 空闲**，栈顶集中在
   `onEvent`/`matchTaker`/`WalHook.appendWal`/`Long2IntMap.get`——≈4.95M 是单撮合线程
   在完整职责（70% 撤单撮合 + WAL 批量 → Archive + 深度发布 + 出站）下的真实上限，
   producer 与 IPC 环只是恰好与它锁步（退避均 <0.1%）。

扩容方向见 §1.4：多 symbol/多端点分片，让多个 `Match-Handler` 线程并行（每 symbol
一条撮合线程 + 独立入站流），或拆多个媒体驱动实例。

--- | --- | --- | --- | --- |
| 单 producer，目标 7M | 4,319,698 | 0 | 48,475（<0.1%） | 单 UDP 端点 → 单 `aeron-order-in` 线程 → 单 `Match-Handler` 线程 |
| 双 producer，目标 3.5M×2 | 2,898,880（1,605,555 + 1,293,325） | 0 | 各 ~5.2 万（<0.1%） | 两 producer 独立 session 均成功连入，合计吞吐反降 |

**Aeron 多 producer 语义（官方文档）**：同一 channel+stream 上的多个
`ExclusivePublication` 各自独立 sessionId，订阅方以 Image 聚合、可用
`header.sessionId()` 区分；排序仅在单 Image 内保证，跨 session 无总序（订单入站无碍）。
参见 [Channels, Streams & Sessions](https://aeron.io/docs/aeron/aeron-channel-stream-session/)；
媒体驱动默认 DEDICATED 线程模型，Conductor/Sender/Receiver 各一线程，Receiver 单线程
poll 所有 UDP socket（[Aeron performance cookbook](https://aeron.io/docs/cookbook-content/aeron-increase-performance/)）。

**归因修正（§1.1 链路多点饱和）**：双 producer 总吞吐不升反降（4.32M → 2.90M），
且两跑 JFR 中 `aeron-order-in` 与 `Match-Handler` 线程采样密度一致（均满负荷，
栈顶集中在 `copyMemory`/`matchTaker`/`WalHook.appendWal`）、producer 端 0 失败，
说明瓶颈不在注入端，而在**单条单线程消费链**：单 UDP ReceiveChannelEndpoint（媒体驱动
Receiver 单 agent）→ 单 `aeron-order-in` 线程（`sub.poll(handler, 256)` 合并全部 image）
→ 单 `Match-Handler` 线程。多 producer 只增加每消息固定开销（多 image 轮询、多 session
协议/流控帧、批处理连续性被打断），因此 ≈4.3M 是「单接收端 + 单消费线程 + 单撮合线程」
的组合上限。扩容方向见 §1.4：多 symbol/多端点分片，让多个 ReceiveChannelEndpoint 与
多个撮合线程并行，或拆多个媒体驱动实例。

### 1.6 全形态验证：输入 journal 入站录制（2026-08-29）

按类型解码 + 异步 WAL + 入站 journal（06-crash-recovery.md §1）落地后的完整压测形态验证
（`stress-test.sh --local --bare-core --big --tps 7000000 --duration 65`，
产物 `/tmp/perf-wal-async`；对照基线 `/tmp/perf-slotid`，同参数、无入站录制）：
（注：本轮异步 WAL 仍存活；archiveWal 已于同日后续整体退役，见 06-crash-recovery.md §1.1/06-crash-recovery.md §1.5。）

| 指标 | 基线（无入站录制） | 全形态（入站 journal） |
| --- | --- | --- |
| 注入 TPS（avg） | 6,827,030 | 5,438,614（稳态 ~5.0M） |
| 入站录制 | — | 121.8GB / 380.7M 行（320B/行，≈1.6GB/s 落盘） |
| WAL 录制 | 8.5GB | 7.2GB |
| Match-Handler 解码段采样 | 37.7%（`OrderInDecoder.id` 单叶子） | 28.7%（`IoCodec` 静态偏移，SBE accessor 已内联） |
| Match-Handler 快照采样 | 0 | 1.0%（10s 周期簿快照，撮合线程同步写） |
| Match-Handler 空闲 | 0.0% | 0.0%（随输入锁步，非瓶颈证据） |
| 背压退避 | 54,188 | 51,661（<0.02%） |

**无 journal 对照（2026-08-29 晚，`--prop -Docx.archive.record-inbound=false
-Docx.snapshot.interval-ms=0`，其余同参数）**：

| 目标 TPS | 实际注入 | 背压退避 | Match-Handler 空闲 | 结论 |
| --- | --- | --- | --- | --- |
| 7M | 7,000,057（精确打满） | 16,987（<0.004%） | 0.0% | producer 目标即上限，引擎未饱和 |
| 8M | 7,564,135（被限速） | 55,608（<0.02%） | 0.0% | **单撮合线程上限 ≈ 7.5M**（JFR 满负荷 + producer 背压） |

无 journal 形态比带 journal 高 ~39%（7.56M vs 5.44M）；瓶颈回到撮合线程本身。
开关：`-Docx.archive.record-inbound=false`（OcxArchiveMain 统一），快照
`-Docx.snapshot.interval-ms=0`；`stress-test.sh --prop` 透传。
产物 `/tmp/perf-no-journal`（7M）、`/tmp/perf-no-journal-8m`（8M）。

**结论**：

1. **解码与 WAL 优化生效**：`OrderInDecoder.id` 叶子（37.7%）消失，解码段降至
   28.7% 且已内联；WAL 的 publication offer/背压全部移出撮合线程（异步 ring +
   `WalJournalThread`），撮合线程只留批内编码；
2. **全形态上限由入站录制端决定**：新增 Archive 入站 recording 是 IPC 环上最慢
   消费者（每 264B 行实占 320B recording 字节、逐帧处理 + 落盘 ≈ 1.6GB/s），
   producer 被环限速到 ~5M，`Match-Handler` 随输入锁步（0% 空闲但仍有处理余量，
   单线程实测可上探 ~7M；WAL 卸载后已实测 ≈8.5M，见 §1.7）；
3. **快照成本可忽略**：10s 周期簿快照在撮合线程仅占 ~1% 采样；
4. **换取的是持久性**：崩溃恢复 = 快照 + 入站回放（06-crash-recovery.md §1.4），380M 行 / 121.8GB
   全量落盘、零丢失（recording 位置与 producer 发送逐字节对齐）；
5. **上限突破方向**：入站录制是磁盘带宽/逐帧处理瓶颈——分 symbol 录制到独立
   介质、放大 `segmentFileLength`、或录制端采样/降频（牺牲恢复完整性），
   需按业务持久性要求权衡。

### 1.7 WAL 卸载后单线程探顶（2026-08-29）

archiveWal 退役后连续探顶（bare-core 裸跑、无 journal、关快照、70% 撤单混合、
`--duration 65`；产物按轮清理，仅保留 JFR 分析结论）：

| 目标 | 拓扑 | 实际注入 | 判定 |
| --- | --- | --- | --- |
| 8M | 1 producer | 8,000,000 精确打满 | 引擎仍有余量（~6% 头寸） |
| 9M | 1 producer | **8,503,764** | 引擎饱和：Match-Handler 100% 采样、producer 退避 0.009%（可忽略） |
| 10M | 2 producer × 5M | **8,582,142** | 引擎封顶：producer-1 精确 5M（retries 5.7K），producer-2 被压至 3.58M（retries 59K） |

**结论：本机（10 核/24GB）单撮合线程上限 ≈ 8.5M TPS，瓶颈是 `Match-Handler`
单线程，与 producer 数量无关**（双 producer 只 +0.9%：多 image 轮询固定开销抵消
吞吐增益，同 §1.5「多 producer 无解」）。引擎侧证据：两轮 JFR 采样 100% 落在
`Match-Handler-BTCUSDT:`，无 idle；producer 侧退避可忽略，非注入端瓶颈。

封顶时撮合线程热点（10M-2p 轮，5815 采样）：

| 热点 | 占比 | 备注 |
| --- | --- | --- |
| `IoCodec.decodeOrderInFields` + `readOrderIn` | ~19% | 解码系仍是第一成本（status 先行裁剪后仅剩静态偏移直读） |
| `OrderBook.matchTaker` | ~17% | 吃单主循环（70% 撤单 + 10% 市价混合） |
| 编排：`onOrderIn` + `validate` + `onFragment` | ~21% | 逐消息分派/校验 + Aeron poll 帧遍历 |
| `LevelSlots.findUpdate` + `getOrCreate` | ~9% | 价格档跳表定位 |
| `TriggerSlots.checkAndTrigger` | 4.6% | 条件单触发扫描 |
| `Long2IntMap.get/tryPut/remove` | ~6% | **仅剩 TriggerSlots 条件单 orderId→slot 索引**（02-data-structure.md §1.6 同构改造 TODO）；主簿已零哈希 |

与 WAL 时代对比：主簿撤单定位哈希与 `appendWal*` 已整体消失，剩余哈希成本全部
来自条件单索引。**继续突破需多 symbol/多实例分片**（每 symbol 独立撮合线程 +
独立入站流，§1.4 候选 3）——单线程 8.5M 即本拓扑物理上限。

**真实撤单模型基准（2026-08-31）**：`CoreMatchBenchmark` 同步 Rust producer 的
真实撤单模型（一单一撤 + 指数寿命，撤单命中率从 ~2% 提到接近真实）后复跑——
默认模式（含生成开销）3,201,900 TPS、撤单占比 51.8%（真实消息流区间）、在簿
147 万稳定；**pregen 纯撮合上限 8,249,463 TPS，40s→90s 完全平线**（8.21M→
8.25M，在簿 98.8 万稳定，90s 成交 2.62 亿笔）——与旧模型探顶 8.5M 量级一致，
且不再被在簿增长拖累。

### 1.8 MediaDriver sender 线程：LowLatency 对照（2026-08-29）

**背景**：UDP 完整形态（五进程 + Archive）压测中 MediaDriver `sender` 线程
≈92% 单核，一度怀疑是架构偏离或可优化点。调研 Aeron 官方架构文档确认：
**sender 本来就是全 driver 唯一的 agent 线程**，负责扫描所有
`NetworkPublication`、在 `SendChannelEndpoint` 上统一发送 DATA/HEARTBEAT/SETUP、
处理 SM 流控与重传，所有 publication 复用同一 socket——单核 92% 是其本来面貌，
**没有偏离架构意图**。官方 Best Practices 的优化手段是 LowLatency 配置
（DEDICATED + conductor BusySpin + sender/receiver NoOpIdleStrategy）加线程绑核，
并强调必须设置 `errorHandler`（否则 driver 故障静默）。

**落地开关**（`OcxMediaDriverMain`，默认保持 Aeron 默认 BackOff 省电配置）：

| 开关 | 作用 |
| --- | --- |
| `-Docx.driver.low-latency=true` | DEDICATED + BusySpin/NoOp idle + 稀疏文件关闭（等价官方 LowLatency 配置），空闲也 100% 占核 |
| `-Docx.driver.pin-base=N` | conductor/sender/receiver 分别钉到 N、N+1、N+2 号核（Linux 生效，macOS no-op） |
| `errorHandler`（无条件） | driver 内部故障不再静默（send-channel 失败、资源耗尽等） |

压测脚本透传：`stress-test.sh --big --driver-low-latency --driver-pin-base N`。

**对照实验**（UDP 完整形态，5M 目标 × 45s，`--no-jfr`，其余参数完全一致，
产物 `/tmp/perf-sender-baseline` / `/tmp/perf-sender-ll`）：

| 配置 | 实际注入 TPS | 发送失败 | 背压退避 |
| --- | --- | --- | --- |
| 基线（默认 BackOff） | **4,143,070** | 0 | 39,545（0.019%） |
| LowLatency + pin-base 1 | 2,649,500 | 0 | 40,759（0.031%） |

**结论：官方 LowLatency 配置在本机（M4 10 核 4P+6E）是负优化（-36%）**。原因：
NoOp 忙等让 driver 三个线程永不退让、空闲也各占满一核；OpenHFT 绑核在 macOS
上为 no-op（无 `sched_setaffinity`），自旋线程抢占 P 核，把真正决定上限的
`Match-Handler` 撮合线程挤到 E 核，整链塌缩。这同时证明 **sender 不是独立瓶颈**，
本机天花板仍是「4 个 P 核 + 单撮合线程」（全形态 ≈4.2M / 裸核 ≈8.5M）。
开关保留供 Linux 服务器使用（可绑核 + P 核充足时 LowLatency 才有收益）；
**本机保持默认 BackOff**。

### 1.9 长跑稳定性与衰减归因（2026-08-30：600s 基线 + 三组对照）

**基线（完整形态 5.5M × 600s，录制+快照+JFR+`--wal-recover-check`，产物
`/tmp/perf-full-55-600s`）**：

- 注入 968,746,000 帧 / 605s，**0 失败**，背压退避 0.05%；**注入 TPS 均值 1,601,233**，
  形态为随长跑衰减：~100s ≈ 3.7M → ~200s ≈ 2.5M → ~600s ≈ 1.6M；
- 全程 10 个监控采样 overloaded=0 / lag=0 / gc_full=0，gc_young 仅启动期 1 次
  （纵表零分配 + 快照/metrics 移出热路径后 GC 不再是变量）；
- Archive：入站 recording 全程 ~310GB、快照回收后仅保留 **754MB**（06-crash-recovery.md §1.6 磁盘水位
  设计的长跑验证）；成交 21GB；快照 recording 60+ 份保留 2 份（随簿增长每份
  714–772MB）；DLT 0；
- 恢复验证：WAL_RECOVERY_DONE，最后快照锚点后增量回放 2,356,876 帧，
  `openOrders=158,971` + `triggerOrders=6,614`（与尾部大量撤单后的终态一致）。

**三组对照（5.5M × 300s，单变量 vs 基线）**：

| 形态 | 注入 TPS 均值 | vs 基线 | 结论 |
| --- | --- | --- | --- |
| 关入站录制（`-Docx.archive.record-inbound=false`） | **4,467,760** | **+2.8 倍** | 全程平线 4.2–4.5M 无衰减，触到完整形态上限 ≈4.3–4.4M（§1.1/§1.5） |
| 关快照（`-Docx.snapshot.interval-ms=0`） | 1,797,624 | +12% | 快照非主因；且关快照后入站 recording 不再回收（300s 留 175GB） |
| 关 JFR（`--no-jfr`） | 1.12M / 复跑 1.17M | 无效 | 对照组被机器热节流污染（越靠后越慢），JFR 写盘 MB 级逻辑上排除 |

**零落盘 600s（2026-08-31）**：`record-inbound=false + record-outbound=false +
snapshot.interval-ms=0`，6g 堆、池子 27.5M 帧，5.5M 目标 × 600s：

| 形态 | 注入 TPS 均值 | 关键证据 |
| --- | --- | --- |
| 零落盘 600s（全 recording/快照关，6g 堆） | **2,658,770** | 1,608,556,000 帧 0 失败；全程 overloaded=0/lag=0/gc_young=0/gc_full=0；Archive recordings=0；成交 3.556 亿笔 |

比"仅关入站录制 600s"（3.15M）低 ~15%（该轮与后续轮的差异主要来自压测撤单模拟
导致在簿增长，正确归因见下节——并非 6g 堆/热节流：4g 堆同配置 TPS 曲线几乎重合、
`pmset -g therm` 无热警告、vm_stat 内存稳定）。完全零落盘下**吞吐不再被录制端
主导**，进入撮合/传输平衡区。过程中修复零落盘形态暴露的 egress 缺陷：
`record-outbound=false` 后 Archive 无订阅者，egress 出站 publication term 填满后
offer 永久负值——原实现纯自旋烧满 2 核、把出站队列/环堵死并回压饿死入站链路
（producer 卡 ~100 万帧）；现改为**无订阅者丢弃本帧**（一次性事件告警）+ 真背压
退避（每 1024 spin park 1ms）。

**长跑衰减归因与真实撤单模型（2026-08-31）**：

零落盘 600s 的平滑线性衰减（4.05M→2.65M）经三轮对照定位为**在簿规模增长驱动**
（非热节流/内存/GC/传输丢包——pmset 无热警告、vm_stat 恒定、0 丢包、0 GC）：
bare-core 下撮合线程 100% 仍 TPS 降（单帧成本随簿增长）；UDP 形态 media driver
单线程 70-96% 是天顶（~4.2M），撮合线程未满（57-81%）。在簿增长根源是**压测撤单
模拟失真**：撤单目标从近窗随机重复选取，每笔限价单平均被撤 50+ 次、首个命中其余
全 miss（命中率 ~2%）→ book 净增长至 cap。

**已改为真实市场撤单模型**（调研：SEC cancel-to-trade ≈ 23:1、>95% 限价单最终
被撤；GDAX 2/3 的 BTC 限价单 2s 内被撤；寿命幂律/指数）：每笔限价单登记一次
到期撤单（一单一撤），寿命指数分布（`--order-lifetime-secs`，默认 2.0s）；池子
模式把寿命截断到池内剩余空间并均匀分散（池子窗口 ≥ 寿命时零失真）；帧构成自动
变为真实比例（撤单 ≈ 限价单数，消息流撤单占比 ~40-45%）；`--cancel-ratio` 废弃。

**验证（零落盘，5.5M 目标，池子 27.5M 帧）**：

| 时长 | 在簿 | TPS 曲线 | 成交 |
| --- | --- | --- | --- |
| 300s | 稳定 51–130 万 | 98s 后平线 ~4.28M（±0.5%） | 4.84 亿笔 |
| 600s | 稳定 29–156 万（池子循环周期波动） | 98s 后平线 **4.17–4.20M（±1%）**，均值 4,218,389 | 9.31 亿笔 |

两轮均 0 失败/0 过载/0 lag/0 GC、Archive recordings=0——**长跑衰减随撤单模型修复
而消失**（对比旧模型 600s：4.05M→2.55M，-37%）。

**完整形态 30 分钟长跑（2026-08-31，新撤单模型）**：生产默认配置（入站录制 +
成交/深度/DLT recording + 快照全开），5.5M 目标 × 1800s：

| 指标 | 数值 |
| --- | --- |
| 注入 | 2,084,105,000 帧 / 1805s，**0 失败**，背压退避 0.071% |
| TPS 均值 | **1,154,628**（88s≈3.3M → 400s≈1.7M → 1000s≈1.27M → 1805s≈1.15M，1000s 后基本收敛） |
| 在簿 / 成交 / 拒绝 | 38–149 万稳定 / 8.04 亿笔（≈44.6 万笔/s）/ 4158 万 |
| 全程采样 | overloaded=0、lag=0、gc_full=0（gc_young 仅 1 次） |
| Archive | 成交 recording 77.6GB（成交率高所致）、入站 recording 快照回收后保留段 ~120MB、快照 2 份（518+388MB） |

完整形态长跑稳态 **~1.15M TPS**（录制端落盘主导，衰减在 ~1000s 后收敛）；磁盘
水位由快照后段回收控制（30 分钟产物 74GB，保留部分与时长解耦）。
**注：该轮运行于旧快照机制（每张快照 start/stop recording，大压力下录制不稳定）；
快照常驻 recording 重构后的 30 分钟大压力验证见下节（方案 A）。**

**快照常驻 recording 重构 + 30 分钟大压力恢复验证（2026-08-31 方案 A）**：

专项定位（Aeron 1.52.2 源码 + 最小复现）：运行中临时 `startRecording` 时 Archive
的 available-image 回调（`availableImageHandler→startRecordingSession`）不触发，
recording 不创建（空载复现、大压力必现）。改为**快照流常驻 recording + BEGIN
分段**（Archive 启动时对快照流录制，每张快照 = `BEGIN(seq)+META+ORDER+STOP+END`，
恢复/对账端逐段校验取最新有效段）——与入站/成交录制同构，彻底根治。

**30 分钟大压力验证（5.5M 目标 × 1800s，完整形态 + 快照 + `--wal-recover-check`）**：

| 指标 | 数值 |
| --- | --- |
| 注入 | 1,931,683,000 帧 / 1805s，**0 失败** |
| TPS | 均值 1,070,184（稳态 1.07–1.26M，录制端主导） |
| 成交 / 拒绝 | 6.49 亿笔 / 5304 万 |
| 全程采样 | overloaded=0、lag=0、gc_young=0、gc_full=0 |
| Archive | 成交 recording 62.3GB、**快照 recording 84GB（常驻累积）**、入站保留段 2.3GB |
| **恢复验证** | **WAL_RECOVERY_DONE**——快照预填 + 锚点增量回放成功，终态 openOrders=1,457,042、triggerOrders=1,048,576 |

**结论：快照常驻 recording 方案在大压力 30 分钟下录制稳定、恢复正确**。遗留：
快照段级回收已落地（2026-08-31，保留最近 2 段，30 分钟大压力从 84GB 压到
受控水位）；成交对账 U5 边界——诊断完成（预填/增量均未见的真未见订单，集中在
压测停止尾部同一 ts，指向 egress/Archive 收尾落盘的成交 ts 尾部语义）——
非对账逻辑（全量路径 RECON_PASS × 4），引擎/egress 尾部 ts 专项另列。

**归因结论**：

1. **入站 recording 的持续落盘是长跑吞吐与衰减的唯一主要来源**：~0.5GB/s
   持续写 + 页缓存累积，随运行时长把吞吐从 ~3.7M 压到 ~1.6M；关掉后 300s 平线
   ~4.4M，衰减消失——与 §1.6「全形态上限由入站录制端决定」一致，且把"持续成本"
   与"长跑衰减"两个效应都归到录制端；
2. 快照贡献 ~12%（0.7GB/10s 写+回收，次要），且承担磁盘水位回收，不可随便关闭；
3. 观测（JFR/GC 日志）无实质贡献；本机 M4 长时间满载存在热节流，长跑对照须
   注意顺序效应（本次 C 组即被污染）；
4. **持续吞吐突破方向 = 录制端落盘搬出主盘**（独立介质/分 symbol 分盘）或录制
   采样/降频（牺牲恢复完整性），需按业务持久性要求权衡（§1.6 结论 5）。

---
## 2. 压测工具链

> 编排/压测脚本统一位于 **`ocx-match-bench/scripts/`**（2026-09-02 自
> ocx-match-core 迁入；仓库根由脚本按 `scripts/../..` 推导，引擎 jar /
> Rust producer 路径自动解析）。下述 `stress-test.sh`/`standby*.sh`/
> `perf-*.sh` 命令均从 `cd ocx-match-bench/scripts` 后执行。

### 2.1 ocx-stress-producer（Rust，单进程 + 订单池）

- 基于 rusteron（Aeron C 客户端 Rust 绑定），静态链接单文件；OrderIn 定长 SBE 帧
  （101B）与 Java 编码器逐字节对齐（黄金帧单测）；
- **单进程**：可跑任意 TPS（100K ≈ 4% CPU）；
- **订单池**：`--pool-size N [--go-file PATH | --go-aeron-dir DIR]`——引擎启动前
  预生成 N 帧（默认 `--pool-secs 10` = 目标 TPS × 10s，大压力探测可显式调小省内存），
  打印 `POOL_READY`；2026-08-30 起本地压测用 Aeron 控制流等 GO（`--go-aeron-dir`
  订阅共享驱动控制流，替代 go 文件握手；远程 producer 仍走 `--go-file`）；
  2026-08-30 补 **status=99 批量撤单**（`--batch-cancel-ratio`，默认 1e-6 ≈ 每秒几次：
  引擎 batchCancel 全簿扫描 O(在簿单数)，按低频运营操作控制比例）；
  热路径每帧只写 8 字节 orderId（撤单再写目标单号），零生成/零分配；
- 池内分布真实：exp 双边深度、tick 量化、市价/条件/冰山/大单混合、价格 regime；
  **撤单为真实市场模型（2026-08-31）**——每笔限价单登记一次到期撤单（一单一撤），
  寿命指数分布 `--order-lifetime-secs`（默认 2.0s，对齐 GDAX/SEC 调研），帧构成
  自动真实化（撤单 ≈ 限价单数，消息流撤单占比 ~40-45%）；**池子窗口
  （`--pool-secs`）须 ≥ 3×寿命均值**，否则池尾订单寿命被截断（均匀分散兜底）；
  `--cancel-ratio` 已废弃；
- Java 版 `StressProducer` 已删除。

### 2.2 stress-test.sh --big

单 producer 池子预生成 → 启引擎（媒体驱动 + Archive + egress + core + ingress）→
GO 控制流（`ControlStream`/`StressControl`，Aeron 控制流替代 go 文件握手；
各进程 READY 亦走同一控制流）→ 连续注入
（预灌+撮合一次跑完）；ArchiveProbe 落盘验证（入站 journal / 成交 / 深度）；
`--wal-recover-check` 时 `RecoveryMain` 从入站 recording 回放验证崩溃恢复；
`--tps/--duration/--pool-secs/
--heap/--cap` 可调。WAL 不再有 `--wal-roll-seconds`/fsync/屏障等 Chronicle 调参。

`--core-only`（2026-08-28 加）：只标定 match-core——不起 egress/sidecar，
出站成交/深度环由新增 `OutboundDrainMain`（只 poll 丢帧、零编码/零持久化）订阅，
避免 `AeronFillSink` 因出站环无订阅者 NOT_CONNECTED 自旋卡死撮合线程；
入站 journal 仍由 Archive 记录、producer 照常。压出来的吞吐即 core 撮合 +
入站 ingest 的极限，与完整链路的差值即 egress 出站编码/持久化成本。

### 2.3 ArchiveProbe

列出 recording（stream/字节/位置），`-Docx.probe.count-trades=true` 时回放成交流
统计 SBE 帧数，与 Prometheus `match_trades_total` 对照。

### 2.4 成交记录对账（2026-08-30，记录级，非结算验证）

`TradeRecordReconciliationMain`：离线回放 Archive 入站 + 成交 recording，用独立于
引擎的轻量模拟器重建 orderId→用户表与专用用户账本，逐笔校验成交记录自洽：

| 用例 | 验证内容 |
| --- | --- |
| U1 逐笔金额 | `price×amount == buy/sellTurnover 增量`（容差 = turnover 舍入 ulp） |
| U2 订单级守恒 | 累计成交量 ≤ 原始量、turnover 单调、买方单 side=BUY / 卖方 side=SELL |
| U3 专用用户账本 | `--fund-users N`（userId<N）：买/卖笔数、base 净持仓、quote 净资金 |
| U4 自成交排除 | buy/sell orderId 映射到不同 userId（STP） |
| U5 引用/撤单 | 无幽灵单；撤单后不得再成交（撮合时间戳与入站同钟后可比） |
| U8 全局守恒 | 成交笔数 == ArchiveProbe 逐帧计数 |

用法：`stress-test.sh --fund-users 8`——producer 生成确定性专用用户场景帧
（STP 同价对 / 跨档大单 / 批撤目标 / 冰山 / 市价 / 条件单），脚本自动关快照
（保留完整入站 recording，避免段回收裁头导致幽灵单误报），停链前跑对账并输出
账本 + RECON_PASS/FAIL。

**对账发现的引擎修复（已落地）**：① cumQuote 跨 taker/maker 阶段单调（03-order-lifecycle.md §1.4）；
② `fill.time` 改用入站帧时间戳——此前成交 recording 用 core 批时钟、撤单 ts 用
ingress 缓存时钟，跨 recording 时间不可比（U5 误报）。

**大压力验证（2026-08-31）**：5.5M × 300s 完整形态（2 producer，424.8M 帧注入，
在簿 5M 触顶、75.6M 拒绝，119,677,225 笔成交）全量对账 **RECON_PASS**——U1/U2/
U4/U5 全 0、U8 笔数一致：**大压力不影响成交正确性**。为此对账器升级为并行合并
回放（入站+成交两路按事件时间推进，内存只随在簿规模走），并修复三处对账模型与
压测设施问题：① Rust producer 双进程 orderId 空间重叠（base 同为 epoch 纳秒，
区间 2.1 亿 → 同 orderId 撞不同帧），按 PID 偏移隔离 ID 空间（`producer_id_offset`）；
② 同 ts 组按入站帧序建模——批撤/撤单只影响其前已创建的单（`createSeq`），条件单
（type 3–6）豁免批撤/单撤（引擎只扫限价簿、不碰 TriggerSlots）；③ 对账 JVM G1 限
4 GC 线程 + 10g 堆（2 线程下 4 亿事件分配后 full GC 超 10s 触发
`ConductorServiceTimeoutException`）。

---
## 3. 关键决策时间线（2026-08 摘要）

| 日期 | 决策 |
| --- | --- |
| 08-21~25 | SBE 二进制化（IO/WAL schema 统一）、数值 primitive 化、批量帧协议 |
| 08-26 | 纵表 SoA 重构（对象图 → 扁平数组）；WAL 纯异步方案 A；FIELDLESS 最小字段 |
| 08-27 | 三进程化（Aeron IPC）；WAL 批末批量帧；撮合线程 idle 策略化 |
| 08-28 | 收单 Aeron UDP 化；出站 Aeron Archive 化（链路完全脱离 Kafka）；Rust 单进程池子 producer；五进程编排；回环标定 600K→2M；**定位 Chronicle 2GiB/4GiB 全文件扫描卡顿根因，WalRollCycle 短周期修复，3M 精确打满** |
| 08-28 续 | 入站 SBE 帧透传（去 decode→encode→decode 往返）+ 路由 int 索引；WAL 批量裸二进制直写（egress CPU 减半）；出站编码免 ThreadLocal + symbol 预编码；4M 精确打满、5M 上限 ≈ 4.3M |
| 08-28 续 2 | **WAL 迁移到 Aeron Archive**：core 直发独立 WAL 流 → Archive recording → `AeronArchive.replay` 回放恢复；删除 Chronicle `WalWriter`/`WalRollCycle`/滚动与屏障配置/`-Ddisable.single.threaded.check`；egress 不再碰 WAL（2GiB 墙根治，5M 上限待复标定） |
| 08-28 教训 | 价格档"纯价格网格/网格+链表"两版在 70% 撤单稀疏档位负载下均退化（758K / 2.03M vs 跳表 4.29M），已回退——**网格只做命中加速、跳表兜底有序索引**才成立（4.36M，+1.6%，因复杂度权衡放弃）；结论：高频撤单 churn 下跳表仍是价格档排序的平衡选择 |
| 08-29 | **订单簿取消 orderId→slot 哈希**：orderId 内嵌代际+槽位（`(gen<<SLOT_BITS)|slot`），簿容量强制 2 的幂（cap=5M→2^23）；入簿/撤单/成交/回放 `id & SLOT_MASK` 直定位零哈希；外部 clientOrderId 与簿内 orderId 两空间，网关按规则换发；冲突按 cap 拒绝可观测 |
| 08-29 续 | **archiveWal 整体退役**：删除 `AeronWalHook`/`TYPE_WAL_BATCH`/`WalJournalThread` 装配与编码，撮合线程不再发布状态变更日志；恢复唯一路径 = 快照 + 入站 journal 回放（`RecoveryService`）；8M 无 journal 对照 7,757,457 → **8,000,000 精确打满**（appendWal 热点消失；8M 时引擎仍有余量，真实上限见 08-29 续 2） |
| 08-29 续 2 | **WAL 卸载后探顶**：单 producer 9M → 8,503,764、双 producer 10M → 8,582,142，**单撮合线程封顶 ≈ 8.5M**（Match-Handler 100% 采样；热点=decode ~19% / matchTaker ~17% / 编排 ~21%；剩余哈希仅 TriggerSlots 条件单索引）；突破方向 = 多 symbol 分片（§1.7） |
| 08-30 | **快照迁往 Aeron Archive recording（Chronicle 快照删除）**：撮合线程批末单遍冻结（实测 ~10-20ms，零分配）→ `SnapshotWriter` 快照线程编码 OCX1 批量帧发布 snapshot 流（每快照一次 start/stop recording）→ Archive 落盘；快照保留最近 2 份 + 入站 journal 锚点前段回收；恢复 = 回放 snapshot recording（META+END+计数校验，无 END 回退上一份）+ 锚点增量回放入站；JFR 确认撮合线程不再有 Chronicle 写停摆，`--wal-recover-check` 恢复计数一致 |
| 08-30 续 | **metrics 队列 Aeron 化（Chronicle 全部退出 match 域）+ 单 JVM 装配根删除**：core/egress 指标（METRICS/EVENT/OUTBOUND_STATS 行）经 metrics 流直发 sidecar；`MetricsQueueAppender`/`MetricsQueueTailer` 删除，sidecar 改为 Aeron 订阅（常驻重试建连）；`MatchEngineBootstrap` 单 JVM 装配根删除；压测 GO 信号改 Aeron 控制流（`STRESS_CONTROL_STREAM`，替代 go 文件）；core 移除 chronicle 依赖 |
| 08-30 续 2 | **长跑归因（600s 基线 + 三组对照）**：完整形态 5.5M×600s 均值 1.60M（3.7M→1.6M 衰减），全程 0 失败/0 过载/0 full GC；关入站录制 4.47M 平线 300s（**+2.8x**，触到完整形态上限），关快照 +12%，关 JFR 被热节流污染——**入站 recording 落盘 = 长跑衰减主因**；快照后段回收把 310GB 录制压到 754MB 保留（关快照 300s 留 175GB）；持续吞吐突破方向 = 录制端分盘/降频（§1.9） |
| 08-30 续 3 | **Aeron 架构审计二轮**：死代码/死配置清理（OcxAeron 三方法、snapshot.enabled/reconcile/retention-days）；引擎 READY 与压测 GO 统一走 Aeron 控制流（`ControlStream`/`StressControl`，替代 .out 文件 grep 与 StressGoSignal，Rust producer 只认 GO 类型）；文档明确架构边界（01-architecture.md §1.5：Cluster/HTTP/metrics 加录/进程内 agrona/配置分发均"有意不 Aeron 化"）；热路径时间戳换 `CachedEpochClock`（ingress 逐帧 → 每 poll 批一次） |
| 08-30 续 4 | **成交记录对账（记录级）**：新增 `TradeRecordReconciliationMain`（回放入站+成交 recording，U1 金额/U2 订单守恒/U3 专用用户账本/U4 自成交/U5 引用与撤单/U8 全局计数）；Rust producer `--fund-users N` 确定性专用用户场景帧；脚本 `--fund-users` 自动关快照保完整入站 + ArchiveProbe 计数做 U8 期望；**对账发现并修复两个资金核算问题**——cumQuote 跨 taker/maker 阶段回落（槽位初始化为已累计额）、`fill.time` 与撤单 ts 不同钟（撮合时间戳改用入站帧时间戳）；冒烟 RECON_PASS（§2.4） |
| 08-31 | **大压力成交正确性验证（5.5M×300s RECON_PASS）**：对账器改并行合并回放（内存随在簿规模、不随总帧数）；修复压测设施 + 对账模型三处——① Rust producer 双进程 orderId 重叠（PID 偏移隔离 ID 空间）；② 同 ts 组按入站帧序建模（批撤/单撤只影响其前已创建的单，`createSeq`）+ 条件单豁免批撤；③ 对账 JVM G1 限 4 GC 线程 + 10g 堆（防 full GC 饿死 Aeron 客户端 conductor）；424.8M 帧 / 119,677,225 笔成交全量对账 **U1/U2/U4/U5 全 0**——大压力不影响成交正确性（§2.4） |
| 08-31 续 | **orderId 三段式（发号方前缀 \| 代际 \| 槽位）**：把当日早上 PID 偏移临时方案升级为结构方案——Rust producer `--instance-id`（每进程独立发号方 ID 1..N）+ 启动纪元随机 token 拼 29 位前缀，低位 34 位计数区（槽 23 + 代际 11，cap≤2^34 不触前缀）；跨进程 / 跨重启 / 将来一实例多币种（多簿）均不撞号，下游对账单键即可；引擎侧零改动（槽位直定位只读低位），压测脚本自动分配 `--instance-id`；生产网关换发 `exchangeOrderId` 仍是 TODO（02-data-structure.md §1.3） |
| 08-31 续 2 | **零落盘形态（record-outbound 开关 + egress 修复）**：OcxArchiveMain 新增 `-Docx.archive.record-outbound`（默认 true，关成交/深度/DLT recording）；配合 `record-inbound=false` + `snapshot.interval-ms=0` 构造纯计算形态；修复该形态暴露的 egress 缺陷——无订阅者时出站 publication offer 永久负值、纯自旋烧 2 核堵死整链（producer 卡 ~100 万帧），改为无订阅者丢弃 + 真背压退避；**零落盘 600s（6g 堆）：均值 2,658,770 TPS、16.09 亿帧 0 失败、全程 0 过载/0 lag/0 GC、Archive recordings=0**（§1.9） |
| 08-31 续 3 | **长跑衰减归因 + 真实市场撤单模型**：三轮对照（bare-core / 全线程采样 / book 触顶）定位衰减主因 = 在簿规模增长（非热节流/内存/GC/丢包）；根源是旧撤单模拟重复选目标、命中率 ~2% → book 净增长；**Rust producer 改为一单一撤 + 指数寿命**（每笔限价单登记一次到期撤单，`--order-lifetime-secs` 默认 2.0s，对齐 GDAX/SEC 调研；`--cancel-ratio` 废弃，帧构成自动真实化）；**零落盘 600s 验证：book 稳定 29–156 万、98s 后 TPS 平线 4.17–4.20M（±1%）**，成交 9.31 亿笔——长跑衰减消失（§1.9） |
| 08-31 续 4 | **完整形态 30 分钟长跑（新撤单模型）**：生产默认配置（录制+快照全开）× 1800s，20.84 亿帧 0 失败，TPS 均值 1,154,628（88s 3.3M → 1000s 后收敛 ~1.15M），在簿 38–149 万稳定、成交 8.04 亿笔、全程 0 过载/0 lag/0 full GC；Archive 成交 recording 77.6GB + 快照回收控制水位（30 分钟产物 74GB）——完整形态长跑稳态 ~1.15M，录制端主导（§1.9） |
| 08-31 续 5 | **快照常驻 recording 重构（方案 A）+ 30 分钟恢复验证**：专项定位运行中 startRecording 的 Aeron available-image 回调缺陷 → 快照流改 Archive 常驻录制 + `BEGIN(seq)` 分段，恢复/对账端逐段校验；**30 分钟大压力（19.32 亿帧 0 失败、成交 6.49 亿笔）恢复验证 WAL_RECOVERY_DONE**（快照预填 + 锚点增量，终态 openOrders=145.7 万）；对账提速 256 倍（poll 1→256）；遗留：快照段回收 TODO、成交对账 U5 边界专项（§1.9） |
| 08-31 续 6 | **快照段级回收落地 + 对账 U5 诊断**：`SnapshotWriter` 记录快照段起点，写后 `purgeSegments` 删最老保留段之前的段（对齐段边界，保留最近 2 段）——300s 完整形态快照 recording 从常驻累积压到 ~774MB、恢复 WAL_RECOVERY_DONE；对账 U5 诊断：预填集合（prefilledIds）区分后确认 U5 为"真未见"订单且集中在压测停止尾部同一 ts——指向 egress/Archive 收尾落盘的成交 ts 尾部语义，非对账逻辑（06-crash-recovery.md §1.6/§1.9） |
| 09-01 | **网关换发落地 + orderId Snowflake 化**：ocx-match-gateway HTTP 收单 → `clientOrderId→orderId` 幂等换发（IdMapping append-only CSV + (seg, counter) 水位持久化，端到端冒烟通过）；发号从「随机 13 位启动纪元」升级为「单调小时段 seg(21) \| 发号方ID(8) \| 代际(11) \| 槽位(23)」——每段 2^34 单、段随自然时间/耗量提前推进（2^21 小时 ≈ 239 年），解除 10k TPS 19.9 天总量墙；时钟回拨 seg 取 max + 同段水位续发；引擎零改动（槽位直定位只读低位）；Rust/Java 测试与 05 02-data-structure.md §1.3 / docs/18 / docs/20 同步（02-data-structure.md §1.3） |
| 09-01 续 | **回退：orderId 纯业务号 + orderIndex 映射 + 槽位 freelist**：复盘发现「ID 内嵌槽位」把业务键与存储位置耦合——轮转冲突税拒绝率 ≈ e^(-CAP/N)（N=1M→0.02%、3M→6%、5M→19%），且发号方无法自由分配；引擎改回 `OrderBook.orderIndex`（Long2IntMap）+ OrderSlots freelist（复用 next[]），判重提前到撮合前（防同号二次成交）；orderId 布局保留 Snowflake 高位（seg\|issuer\|counter 纯业务号）；代价 = 每次入簿/撤单一次 O(1) 哈希 + orderIndex 常驻 ~100MB@5M；**零落盘吞吐复测：389.2 万 TPS（300s）vs 08-31 基线平线 417–420 万 → 约 5–7%，可接受**；核心/网关/Rust 测试全绿（02-data-structure.md §1.3 / docs/18） |
| 09-01 续 2 | **U5 边界专项：快照预填解码 bug 根因 + 修复**：对账锚点模式 U5 尾部幽灵单曾被误诊为"Archive 收尾语义"（08-31 续 6）——实证定位为 **OrderRecordCodec var-data 编解码不一致**（ocx-wal-schema varString 声明在固定字段中间，SBE 生成 accessor 长度槽错位；旧 putString 跳过缺失槽位 + 复用 scratch 残留垃圾 → 快照 ORDER/STOP 记录 100% 解码失败，被 catch(ignored) 静默吞掉 → 预填实际为空；引擎恢复因锚点窗口覆盖全部活单侥幸通过）；修复 = OrderRecordCodec 手写自洽 var 段 + 恢复路径解码失败上报 + 对账预填基线单位修正；锚点模式 **U5=2535→0、U1=913→0，302,982 笔成交 RECON_PASS**；脏 scratch 回归测试防复发（02-data-structure.md §1.3 / docs/18） |

---

