# ocx-match-bench

JMH 微基准模块，与 JFR 配合观测撮合引擎热路径（对应压测 JFR 栈顶）。

## Benchmark 一览

| 类 | 覆盖热路径 | 压测 JFR 对应 |
| --- | --- | --- |
| `OrderBookBench.cancelReadd` | 撤单 + 同价回补：`Long2IntMap.get` 定位、LevelSlots 摘链 | 70% 撤单流主力 |
| `OrderBookBench.marketTaker` | 市价 taker 吃单：`matchTaker` 主循环、多 maker 成交、冰山路径 | taker 成交热点 |
| `OrderBookBench.stressMix70` | 70% 撤单 + 30% 新增限价混合（`@OperationsPerInvocation(10)`） | 与 stress 配置对齐 |
| `OrderBookBench.cancelReadd{Agrona,Custom}{Map,Lf05}` | 撤单定位 map 级 A/B：自研 `Long2IntMap` vs Agrona `Long2LongHashMap` × 负载 0.25/0.5 | 撤单热点归因 |
| `CodecBench.encodeOrderIn` | SBE OrderIn 编码 | ingress 透传 |
| `CodecBench.decodeOrderIn` | SBE OrderIn 零分配直读 | core 撮合线程解码 |
| `AeronWireBench.inboundFrame` | 入站行透传（8B 时刻 + 帧，264B 定长） | ingress 透传发布 |

## 构建

    mvn -Pbench -pl ocx-match-bench -am package -DskipTests
    # 产物: ocx-match-bench/target/benchmarks.jar（shade 可执行包）

> 说明：ocx-match-bench 挂在 `-Pbench` profile 下，默认构建（`mvn package`）
> 完全不含 JMH，生产制品零影响；且本模块已禁用 install/deploy，不会被发布。
> compiler 3.13.0+ 默认关闭 classpath 隐式注解处理，本模块已显式声明
> `jmh-generator-annprocess`（annotationProcessorPaths），否则无 BenchmarkList。

## 运行（JMH + JFR）

    ./bench.sh -l                              # 列出全部 benchmark
    ./bench.sh OrderBookBench                  # 跑类，JFR 录制到 target/jfr/
    ./bench.sh OrderBookBench.cancelReadd -f 3 -i 10   # 更精确的 fork/迭代
    ./bench.sh -prof gc OrderBookBench         # 叠加 GC 分配指标

每个 benchmark 一份 `target/jfr/<bench>-Throughput/profile.jfr`，分析：

    jfr print --events jdk.ExecutionSample \
      target/jfr/OrderBookBench.marketTaker-Throughput/profile.jfr | less
    # 或用 JMC（jmc）打开逐帧查看栈

## 整链编排/压测脚本（2026-09-02 自 ocx-match-core 迁入）

整链编排、主备 HA 与故障演练脚本统一在 `ocx-match-bench/scripts/` 维护：

| 脚本 | 用途 |
| --- | --- |
| `scripts/stress-test.sh --big` | 单机全形态压测（默认带 journal bridge；`--wal-recover-check`/`--fund-users N` 触发恢复/对账收尾） |
| `scripts/standby-test.sh` | 双实例主备：一致校验 / `--failover` 故障接管 / `--drill-*` 演练矩阵 / `--raft` 仲裁 |
| `scripts/standby2-test.sh` | 一主两备 + Raft 3 节点仲裁组 |
| `scripts/standby-longrun-test.sh` | 主备长时间运行 |
| `scripts/raft-arbitration-smoke.sh` | 仲裁组快速冒烟（3 节点 READY/当选/注册） |
| `scripts/perf-disk-sample.sh` / `scripts/perf-flatline-analyze.py` | 磁盘采样 / TPS 平线分析（D 系列复盘工具） |

运行前先构建引擎与 Rust producer，然后从 scripts 目录执行：

    cd ocx-match-bench/scripts
    ./stress-test.sh --big --tps 1500000 --duration 1800 --heap 6g --cap 5000000 \
      --fund-users 100 --wal-recover-check --no-prom --no-sidecar \
      --prop -Docx.snapshot.interval-ms=60000

## 与整链压测的关系

- JMH 单测各原子热路径（撮合/编解码/入站行透传），给出**每操作成本**与
  理论吞吐上限（含 JFR 采样确认热点归属）；
- `stress-test.sh --big --core-only --bare-core` 测整链端到端上限（当前
  单撮合线程 ≈4.95M TPS）；两者互相印证：JMH 单路径成本 × 消息混合比 ≈
  端到端上限。

## 观测记录（2026-08-29，bare-core 单 producer 4.906M TPS）

> archiveWal 已于 2026-08-29 同日整体退役（§4.1），`AeronWalHook.appendWal`
> 热点与下方 ② 节为退役前归因证据，保留存档。

端到端（`Match-Handler-BTCUSDT` 线程 JFR，4131 采样）前四热点：

| 热点 | 占比 | JMH 单路径成本 | 结论 |
| --- | --- | --- | --- |
| `AeronWalHook.appendWal` | 22.3% | 编码仅 2.3–3.0ns/行 | 大头是 Aeron offer 到 Archive 的发布/背压，非编码 |
| `OrderBook.matchTaker` | 20.7% | 25.7ns（吃 10 个 maker） | 生产吃单深度/冰山/fill 发布高于孤立测量 |
| `IoCodec.readOrderIn`→`UnsafeBuffer.wrap` | 15.7% | decode 合计 8.5ns | **wrap 每次重包是明确可优化点** |
| `OrderBook.cancel`→`Long2IntMap.get` | 13.6% | cancelReadd 73.5ns 中 get 占主 | 70% 撤单下的结构性哈希成本 |

结论：204ns/消息预算中，纯簿内操作（JMH 可覆盖部分）仅 ~72ns（35%），
剩余 ~65% 是消息级固定开销——解码、WAL 发布/背压、Aeron 环往返；
瓶颈在**外围（解码+WAL 发布）而非簿内撮合**。

## 撤单定位 A/B（2026-08-29，Long2IntMap vs Agrona，同容量同 live）

独立 map 基准镜像 `orderIndex` 的「get→remove→put」回补环（2026-08-29 曾尝试将槽位
内嵌进 orderId 以省去哈希查找，2026-09-01 因回绕冲突税 e^(-CAP/N)（N=5M 时约 19% 拒单）
及发号方无法自由分配而回退；当前 `orderIndex` 即自研 `Long2IntMap`，为撤单 / addOrder
主路径热点）：容量 2^20 /
live 262144（负载 0.25），LF05 组容量减半至 2^19（负载 0.5）。Agrona 取
`Long2LongHashMap`（2.5.0 无 long→int 变体，值 8B 较自研 4B 略吃亏）；
删后回补同键，各表不扩容、无墓碑风暴。

| 基准 | 实现 | 负载 | 吞吐 ops/ms | 每 cycle |
| --- | --- | --- | ---: | ---: |
| `cancelReadd` | 完整簿路径（自研 map） | 0.25 | 15,747 | 63.5ns |
| `cancelReaddCustomMap` | 自研 `Long2IntMap` | 0.25 | 69,837 | 14.3ns |
| `cancelReaddAgronaMap` | Agrona `Long2LongHashMap` | 0.25 | 79,070 | 12.6ns |
| `cancelReaddCustomMapLf05` | 自研 `Long2IntMap` | 0.5 | 41,734 | 24.0ns |
| `cancelReaddAgronaMapLf05` | Agrona `Long2LongHashMap` | 0.5 | 32,384 | 30.9ns |

> **读数注意**：上表 `cancelReadd` 行（63.5ns）为 **2026-08-29 内嵌槽位（零哈希）窗口期**实测；该路径于 2026-09-01 已回退为 `Long2IntMap`（`orderIndex.get` 重新进入热路径，占压测 JFR 自采样 36.2%），故**当前 `cancelReadd` 数值高于 63.5ns**（见本轮复标）。独立 map A/B（`cancelReaddCustomMap` / Agrona 系列，不依赖簿内结构）的负载因子结论仍有效。

JFR 归因（同负载 0.25）：自研 map 采样分散在 get/remove/put 三处探测；
Agrona 约 88% 采样压在 `remove`+`compactChain`（删除即整簇左移），换来
get/put 探测恒短。

结论：

- **同负载 0.25 下 Agrona 快 13%**（79.1M vs 69.8M ops/s），实打实但有限；
  而 map 仅占完整撤单路径 ~22%（63.5ns 中 14.3ns），换实现全路径只动 ~±3%，
  **不是杠杆**。
- **负载收紧到 0.5 时自研反超 29%**（41.7M vs 32.4M）：Agrona 的
  `compactChain` 随簇长超线性恶化（0.5 较 0.25 跌 59%，自研只跌 40%）——
  自研「墓碑 + 就地重建」在内存吃紧时更抗压。
- **负载本身是最大变量**：自研 map 0.25→0.5 跌 40%。若生产 `orderIndex`
  实际负载偏高，扩容量（保持 ≤0.25~0.3）是零改代码的立竿见影收益。
- 下一步杠杆不在换 map 实现，而在**免哈希**：撤单消息携带 slot hint
  （省掉 get 探测）、稠密 orderId 直映射数组、近期单热缓存。

### 复标（2026-09-06，09-01 回退后当前代码）

为修正上方存档（零哈希窗口期）并量化回退代价，在当前代码（`orderIndex` = 自研
`Long2IntMap`，撤单 / addOrder 主路径）重跑同一组基准（`-f1 -wi3 -i5 -w5s -r5s`，JDK21）：

| Benchmark | ops/ms | ns/op |
| --- | ---: | ---: |
| `cancelReadd`（完整簿路径，含 `orderIndex.get`） | 8,899.9 | 112.4 |
| `cancelReaddCustomMap`（自研 0.25） | 56,203.7 | 17.8 |
| `cancelReaddAgronaMap`（Agrona 0.25） | 78,346.5 | 12.8 |
| `cancelReaddCustomMapLf05`（自研 0.5） | 38,942.1 | 25.7 |
| `cancelReaddAgronaMapLf05`（Agrona 0.5） | 29,180.8 | 34.3 |

- 零哈希存档 `cancelReadd` 63.5ns → 当前 112.4ns（重新含哈希查找，**约 +77%，但跨期机器状态有差异，仅作量级参考**）；
- **负载因子仍是 map 最大变量**：自研 0.25→0.5 跌 ~44%（17.8→25.7ns）；
- **负载交叉点仍在**：0.25 时 Agrona 快 ~28%（12.8 vs 17.8），0.5 时自研反超 ~25%（25.7 vs 34.3）；
- **map 仅占完整撤单路径 ~16%**（17.8 / 112.4），撤单其余开销（removeSlot / LevelSlots 摘链）为主——印证「换 map 实现全路径仅 ±3%，非杠杆」；
- **零改代码收益**：保持 `orderIndex` 负载 ≤0.25~0.3（扩容量）即可压住 lookup 成本；
- **真杠杆仍是免哈希**（撤单消息带 slot hint、稠密 orderId 直映射数组、近期单热缓存）。

### 两点深挖（2026-08-29 续）

**① `readOrderIn` 的 `UnsafeBuffer.wrap`（JFR 15.7%）**

- JMH 同进程 A/B：wrap 开销 0.85ns/op（decode 8.5→9.9ns 的 9%），
  `decodeOrderInLegacyWrap` 基准保留对照；
- 消除后 JFR 采样从 wrap 迁移到 `OrderInDecoder.id`（SBE getter 调用），
  解码总成本不变——JFR 的 15.7% 主要是不内联 getter 的**采样归因**而非真实 CPU；
- 端到端 A/B（同机器状态，各 2+ 次）：4.906M / 4.883M（旧）vs
  4.907M / 4.766M / 4.759M（新，含一轮噪声）——**持平**，改动保留（解码路径快 9%）。

**② WAL 发布 / Archive 背压（appendWal JFR 22.3%）**

- 4.9M 裸跑 JFR：`offerRetry` 仅 30/4131（0.7%）、`BackoffIdleStrategy` 0 次、
  `recordOutboundBackpressure` 0 次——**Archive 完全跟得上，无背压**；
- JMH 完整周期（真实 `AeronWalHook` + embedded driver + 后台排空）：
  `walPublishCancel` 2.7ns/事件 vs 纯攒批编码 2.0ns/事件 → offer 无背压时
  仅 ~0.7ns/事件；CREATE 批（17 行/帧）6.3ns/事件（批小，固定成本摊得薄）；
- appendWal 的 22% 采样主要是 method sampler 对「小而频繁」方法的
  入口/出口命中偏差（吞吐低的 run 采样占比反而更高，与真实 CPU 负相关）；
  真实成本 ≈1-2%。**背压不是 4.9M 下的瓶颈**。

真正结构性成本仍在撮合簿：`Long2IntMap.get`（撤单定位，14-24% 随轮次波动）
+ `matchTaker` + fill 事件链——与文档 §10.2 结论一致。

### ③ 观测税 A/B：full / batch / off（2026-08-29，bare-core 同参数）

验证「消息级固定开销」里时钟+指标占多少：新增
`-Docx.match.metrics.mode=full|batch|off`（`MatchMetrics.Mode`）：

- `full`：逐单 `2×System.nanoTime` + `recordMatch`（CHM 查找 + HdrHistogram + LongAdder
  + `currentTimeMillis`），即原始语义；
- `batch`：撮合线程只做批级累加（每批 1 次时钟 + 每型 1 次直方图记录），
  `OrderEventHandler` 批末回调 `MatchEngine.onBatchEnd()` 提交；
- `off`：完全跳过逐单时钟与指标（最终 TPS 仍可从 producer 侧对账）。

| 模式 | 注入 TPS | 相对 full | JFR 佐证（Match-Handler 采样） |
| --- | --- | --- | --- |
| full | 4,856,053 | 基准 | `onOrderIn` 2465 次（时钟+指标算到调用者头上）；`recordMatch` 44 次 |
| batch | 5,303,961 | **+9.2%** | `onOrderIn` 2160；`recordBatchMatch` 仅 9 次 |
| off | 5,459,776 | **+12.4%** | `onOrderIn` 1775；`recordMatch` 0 次 |

结论：**观测税 ≈12%（+60 万 TPS）**，`batch` 模式拿回其中 ~74% 的收益
（9.2/12.4）且保留完整计数指标（延迟降为批均值，4.9M 下逐单本就近 0ms）。
其余固定开销（Aeron poll/解码/WAL 打包）为消息机制税，`off` 也省不掉。
**生产默认已切 `batch`**（`modeFromProperty()` 未配置时回退 BATCH）；
`full`（逐单直方图）需显式 `-Docx.match.metrics.mode=full`，仅压测对照/逐单
延迟观测需要。

产物：`/tmp/perf-obs-{full,batch,off}/core.jfr`（三档 JFR 保留）；
模式开关（脚本位于 `ocx-match-bench/scripts/`）：
`./scripts/stress-test.sh --big --bare-core ... --metrics-mode off`。

## 观测记录（2026-08-30，快照/metrics Aeron 化 + 撤单零哈希后复标）

> 同参数 `-f 1 -wi 3 -i 5 -w 5s -r 5s`（1 fork，JFR 在 target/jfr/）。
> 与 08-29 存档对比：撤单定位哈希（orderId 内嵌槽位）与观测/快照开销的收益仍在。

| Benchmark | 吞吐（ops/ms） | 单操作成本 | 08-29 存档对照 |
| --- | --- | --- | --- |
| AeronWireBench.inboundFrame | 216,742 | 4.6 ns | — |
| CodecBench.decodeOrderIn | 91,029 | 11.0 ns | decode 合计 8.5ns（旧路径） |
| CodecBench.decodeOrderInLegacyWrap | 81,451 | 12.3 ns | wrap 重包路径 |
| CodecBench.encodeOrderIn | 26,138 | 38.3 ns | — |
| OrderBookBench.cancelReadd | 15,656 | 63.9 ns | 73.5ns（含哈希 get） |
| OrderBookBench.marketTaker | 42,750 | 23.4 ns | 25.7ns |
| OrderBookBench.stressMix70 | 18,049 | — | — |

完整形态端到端（`--local --big --tps 5500000 --duration 30 --wal-recover-check`，
产物 `/tmp/perf-full-55-verify`）：实际 2.68M TPS（35s 窗口含冷启动爬坡），
Prometheus 实时指标走新 metrics 流（sidecar 直订阅），快照 recording 恢复
`openOrders=228,002` + `triggerOrders=17,493`（锚点后增量回放 8.1M 帧）。
