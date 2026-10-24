# 撮合引擎可观测性改造与压测结论

> **本章解决**：历次压测的完整记录与结论（附录性质，可跳读）。
> **前置依赖**：10（性能基线）。
> **读者前置**：无。

> 迁移自 open-cex-docs `references/observability-stress-test-report.md`（2026-09-05 迁入 open-cex-match/docs）。

> ⚠️ 归档注记（2026-09-02）：本报告为历史观测/压测专项文档，已由
> 05-recording-decoupling.md（D 系列）与 match-engine-*.md 接替，仅作历史参考，不再更新。

> 最后更新：2026-08-28 | 状态：已部署并验证 | 相关：`match-engine-*.md`、`11-runtime-runbook.md`
>
> **口径提示**：本报告为 2026-08-28 WAL 迁移（Chronicle → Aeron Archive recording）
> **之前**的历史压测结论。涉及 WAL 的部分（`WalWriter` 写线程、flush 屏障、
> `-Ddisable.single.threaded.check`、Chronicle 滚动等）已随迁移失效；指标体系
> （metrics queue + sidecar）与 4M 以下基线仍有效。WAL 现状与最新标定见
> `06-crash-recovery.md` §4 / `10-performance.md` §10。

## 1. 背景

把撮合引擎改造成 **headless**（不监听入站 HTTP、不主动 push），可观测性改为 **Chronicle Queue IPC + 独立 metrics exporter（sidecar）**。引擎后台线程把指标快照 append 到 Chronicle Queue，sidecar tail 并暴露 `/metrics`，Prometheus 抓 sidecar。

## 2. 可观测性改造要点

- 引擎：`spring.main.web-application-type=none`，关闭 web/actuator 入站端口。
- 指标写入：`MetricsQueueAppender` 每 2s 写 idle 心跳、每 5s 写完整快照（Chronicle 原生 binary wire），过载时只写心跳；心跳和快照都携带引擎进程级 JVM 指标（heap/thread/GC pause）。
- sidecar：新增 `ocx-metrics-exporter` 模块，tail 各引擎 queue、按 `profile`+`symbol` 渲染 `/metrics`（端口 9101）、超过 10s 无新记录置 `match_engine_up=0`。
- Prometheus：match-engine 抓取目标改为 sidecar；105 本机抓 `localhost:9101`，当前抓 120 用 `192.168.0.120:9201`。
- Grafana：面板改为 `profile`+`symbol` 双变量，14 个面板（含“引擎存活/引擎过载”、JVM Heap、GC Pause、Disruptor Batch Size Max）。
- 告警：改为 `match_engine_up` / `match_engine_overloaded` / 新指标名，并按 `symbol`/`profile` 打标。

## 3. 压测过程中发现并修复的 bug

1. **spring-kafka 版本冲突**：`spring-kafka 3.2.4` + `spring-integration-kafka 6.3.5` 导致 `NoClassDefFoundError: KafkaMessageListenerContainer$TriggerCallback`，`@KafkaListener` 容器起不来（consumer 只 poll 不触发业务）。
   - 修复：父 pom 锁定 `spring-kafka 3.2.10`（3.2 系列最新补丁，兼容 Boot 3.3.5）。
2. **`MessageConsumerListener` 构造器里启动采样线程**：访问尚未注入的 `matchMetrics` 触发 NPE，TPS/lag 采样线程死亡。
   - 修复：采样线程挪到 `@PostConstruct`。
3. **`ConsumerLagProbe` 把“查不到分区”误当“零积压”**：`partitionsFor(topicPattern)` 只支持精确 topic 名，对正则返回空列表时返回 0。
   - 修复：改为 `listTopics()` + 正则匹配，查不到返回 `-1`。
4. **`StressProducer` 价格不满足 tickSize**：`double` 随机价格带长小数，被引擎 tickSize（1e-8）校验拒绝。
   - 修复：价格/数量 `setScale(8, HALF_UP)` 对齐 1e-8。

## 4. 压测结果

### 4.1 单实例（spot/BTCUSDT，quick 阶梯）

| 场景 | 注入 | consumer lag | 消费 TPS | 撮合订单 | 成交 | 拒绝 | RingBuffer |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke 1K×30s | 3 万 | 0 | ~1016 | 29,464 | 22,399 | 0 | 健康 |
| load 10K×60s | 60 万 | 1000 | 10,400 | 610,884 | 477,032 | 0 | 健康 |
| stress 50K×60s | 300 万 | 202 万 | 13,669 | 661,200 | 516,459 | 0 | 健康 |

结论：单实例下消费端是瓶颈（约 10~14K TPS），撮合核心（Disruptor）没有成为瓶颈，P99 撮合耗时 <1ms，零误拒绝。

### 4.2 四实例并行（每实例 10K TPS × 45s，共 40K TPS / 180 万单）

| 实例 | up | overloaded | RingBuffer | consumer lag | 拒绝 |
| --- | --- | --- | --- | --- | --- |
| spot/BTCUSDT | 1 | 1 | 0（满） | 357,256 | 0 |
| spot/ETHUSDT | 1 | 1 | 0（满） | 365,140 | 0 |
| futures/BTCUSDT | 1 | 1 | 0（满） | 356,040 | 0 |
| futures/ETHUSDT | 1 | 1 | 0（满） | 370,240 | 0 |

结论：4 个实例全部存活、全部正确判定过载、零误拒绝；但 **RingBuffer 全部打满（remaining=0）**，说明这次瓶颈从“消费端”转移到了“撮合核心”。

### 4.3 四实例并行（120，每实例 10K TPS × 45s，共 40K TPS / 180 万单）

在 120（Apple M4，10 物理核）清理 Kafka/WAL 积压后重新压测：

| 实例 | up | overloaded | RingBuffer | consumer lag | 拒绝 |
| --- | --- | --- | --- | --- | --- |
| spot/BTCUSDT | 1 | 0 | 65536 | 0 | 0 |
| spot/ETHUSDT | 1 | 0 | 65536 | 0 | 0 |
| futures/BTCUSDT | 1 | 0 | 65536 | 0 | 0 |
| futures/ETHUSDT | 1 | 0 | 65536 | 0 | 0 |

每实例约 45 万 `match_orders_total{type="limit"}`、35 万 `match_trades_total`。
结论：同样的 40K TPS 总吞吐在 120 上轻松通过，`match_engine_overloaded=0`、`ringbuffer_remaining=65536`、`consumer_lag=0`，
说明 105 的四实例打满是 2 物理核导致的 CPU 争抢，不是撮合代码瓶颈。

### 4.4 四实例并行（120，每实例 20K TPS × 90s，共 80K TPS / 720 万单）

本轮是在“BigDecimal 定点化 → 去 BigInteger → TreeMap/TradePlate 原始 long 键 → IoCodec ThreadLocal 缓冲 → Reconciler 轻量化”这一系列优化后重新压测。

| 实例 | up | overloaded | 堆 used | GC 次数 | 平均 GC 停顿 | 最大 GC 停顿 | Full GC |
| --- | --- | --- | --- | --- | --- | --- | --- |
| spot/BTCUSDT | 1 | 0 | ~534MB | 162 | 7.5ms | 41.2ms | 0 |
| spot/ETHUSDT | 1 | 0 | ~529MB | 164 | 7.2ms | 32.4ms | 0 |
| futures/BTCUSDT | 1 | 0 | ~568MB | 151 | 8.5ms | 52.5ms | 0 |
| futures/ETHUSDT | 1 | 0 | ~521MB | 158 | 8.3ms | 51.9ms | 0 |

每实例约 180 万 `match_orders_total{type="limit"}`、140 万 `match_trades_total`，结束 `consumer_lag=0`、0 发送失败。

JFR 热点/分配对比（spot/BTCUSDT）：

| 指标 | BigDecimal 版 | 定点化后 |
| --- | --- | --- |
| 连续 Full GC | 300–370ms | 0 次 |
| `Decimal.compareTo` 热点 | 15.77% | ~0.9% |
| TreeMap 键比较 | BigDecimal 对象比较 | 原始 `long` |
| `IoCodec.encodeDepthSnapshot` 分配 | 17.79% | 退出 top 分配站点 |
| `byte[]` 分配占比 | 39.82% | 21.91% |

结论：数值计算全面切换到原始 `long` 后，GC 长停顿消失，撮合热路径不再有单一主导热点。

### 4.5 单实例 JVM 专项（120，20K TPS × 90s，去 Spring + 原始 long 订单簿）

2026-08-23 在 120 上以干净环境（停止 105 `ocx-market-maker`、清空 Kafka/WAL/metrics）完成
`spot/BTCUSDT` 单实例压测，用于验证移除 Spring 相关依赖、WAL 撮合字段数值化以及
`LongObjectSkipList` 原始 long 订单簿后的 JVM 表现。

| 指标 | 结果 |
| --- | --- |
| 发送订单 | 1,800,100 |
| 发送失败 | 0 |
| 实际 TPS | 20,001 |
| 引擎采样订单 | 1,793,100 |
| 成交 | 1,403,119 |
| 最终深度 | bid 195,976 / ask 193,929 |
| consumer lag | 0 |
| heap used（采样） | 约 585 MB |
| GC 总停顿 | 269 ms |
| GC 次数 | 58 |
| 平均 GC 停顿 | 4.64 ms |
| P99 GC 停顿 | 13.3 ms |
| 最大 GC 停顿 | 13.3 ms |
| Full GC | 0 |

与历史 JFR 对照：

| 指标 | Spring 模块仍在（v9） | Spring 已移除、TreeMap 仍用（perf2） | 本版（perf5） |
| --- | ---: | ---: | ---: |
| GC 总停顿 | 8.28 s | 265 ms | 269 ms |
| GC 次数 | 187 | 57 | 58 |
| 最大停顿 | 211 ms | 10.9 ms | 13.3 ms |
| Full GC | 有 | 0 | 0 |

JFR 分配热点（perf5）：

| 分配站点 | 占比 |
| --- | ---: |
| `StringLatin1.newString` | 11.78% |
| `IoCodec.copyExact` | 4.07% |
| `WalWriter.writeCreate` | 3.80% |
| `ByteBuffer.wrap` | 3.62% |
| Kafka `Utils.toArray` | 3.31% |
| `Long.valueOf` | 3.16% |

结论：相比 Spring 模块仍在时，GC 停顿从秒级降到 0.27s、最大停顿从 211ms 降到 13.3ms；
订单簿热路径的 `Long` 装箱已明显收窄，但主要剩余分配集中在 Kafka/协议编解码和 WAL 边界。

### 4.6 单实例 JVM 专项（120，20K TPS × 90s，日志去装箱 + ExchangeTrade.time primitive 化）

2026-08-24 在 120 干净环境（停止 105 `ocx-market-maker`、重建 `spot_in.BTCUSDT`、清空 WAL/metrics）完成
`spot/BTCUSDT` 单实例压测，验证两处热路径改造的 JVM 收益：

1. **日志去装箱**：新增 `DebugLog`（ThreadLocal 复用 StringBuilder，Logback 环境下等价 Log4j2 `Unbox`），
   撮合热路径 debug 日志全部改为构造时缓存的 `logDebug` 布尔保护（对标 exchange-core 的 `boolean logDebug`），
   并删除低价值日志（整单 `Order.toString()`、与发布侧重复的 8 字段 trace）。级别未开启时日志路径零分配。
2. **ExchangeTrade.time primitive 化**：`Long` → `long`，同步修正 IoCodec 编解码、push 端 `TradePushVO.setT0`
   自赋值 bug，并修复 spot/futures/push 对 `buyOrderId/sellOrderId` primitive 化后遗留的编译错误
   （`long != null`、long→String 转换等）；删除废弃 String setter / `encodeCancelAck(String)`
   （`"1".hashCode()=49` 是测试失败与脏数据的根源）。

| 指标 | 结果 |
| --- | --- |
| 发送订单 | 1,800,100 |
| 发送失败 | 0 |
| 实际 TPS | 20,001 |
| 引擎采样订单 | 1,780,677 |
| 成交 | 1,393,010 |
| consumer lag | 0 |
| RingBuffer 剩余 | 65536（无过载） |
| 拒绝 | 0 |
| 撮合耗时 | p50/p95/p99=0ms，p999=1ms，max=3ms |
| heap used（压测结束后回落） | 约 263 MB |

GC 对比（同参数 perf5 基线，JFR 实测）：

| 指标 | perf5（lmax6） | 本轮（lmax7） | 变化 |
| --- | ---: | ---: | ---: |
| GC 总停顿 | 269 ms | 108 ms | -60% |
| GC 次数 | 58 | 33 | -43% |
| 平均停顿 | 4.64 ms | 3.27 ms | -30% |
| 最大停顿 | 13.3 ms | 8.4 ms | -37% |
| Full GC | 0 | 0 | — |

JFR 分配热点对比：

| 分配站点 | perf5 | 本轮 |
| --- | ---: | ---: |
| `StringLatin1.newString` | 11.78%（第 1） | ~2.2%（第 13） |
| `Long.valueOf` | 3.16%（第 6） | ~1.3%，且全部位于 Kafka 客户端 / Spring Boot 加载期 |

结论：日志路径的装箱与消息构造消失后，GC 总停顿再降 60%（269ms→108ms）、最大停顿 13.3ms→8.4ms；
**撮合热路径已无 `Long` 装箱**，剩余分配集中在 Kafka 协议层与 SBE 编解码边界。全程 0 Full GC、0 拒绝，
引擎日志仅 1 条 Kafka at-least-once 重复单告警（180 万分之一，与改造无关）。
JFR 样本：`~/match-jfr/lmax7.jfr`（本轮）/ `lmax6.jfr`（perf5 基线）。

### 4.7 单实例 JVM 专项（120，20K TPS × 90s，出站批量路径免拷贝 + 编码侧缓冲复用）

2026-08-24 第二轮验证。目标：SBE 出站零拷贝。**结论：零拷贝路线被证伪，保留四项真实收益。**

#### 4.7.1 零拷贝尝试与证伪

方案：IoCodec 线程本地编码缓冲 + `ByteBuffer` slice 直发 Kafka（`ByteBufferSerializer`），期望消除每笔
`copyExact`。反编译 Kafka 3.7 `ByteBufferSerializer.serialize` 后确认其行为：

- 仅当 `remaining() == array().length`（即 limit 等于整个底层数组容量）时才直接返回底层数组；
- 否则必然 `new byte[remaining()]` + 拷贝。

slice 的 limit 恒小于缓冲容量，永远走拷贝分支——等价于把 `copyExact` 从撮合代码搬进 Kafka 序列化层，
**净收益为零**，已整体回退（代码未残留）。

#### 4.7.2 保留的优化

1. `OrderBooks.drainTrades/drainCompletedOrders`：改为**交换出新列表**返回，消除每批逐元素拷贝与中间
   `ArrayList`/大数组复制；
2. `flushEndOfBatch`：**编码+发送逐笔合一**，去掉 `sbeTrades` 中间列表；
3. `IoCodec.writeVar`：**symbol 字节缓存**（单引擎 symbol 恒定，消除每笔成交一次 `getBytes`）；
4. 编码路径复用 **ThreadLocal `UnsafeBuffer`**，消除每笔 `new UnsafeBuffer`。

#### 4.7.3 压测结果（lmax7 → lmax9）

| 指标 | lmax7（上轮） | lmax9（本轮） | 变化 |
| --- | ---: | ---: | ---: |
| JFR 分配采样 | 10,738 | 7,565 | **-29.5%** |
| `byte[]` 分配 | 2,178 | 1,562 | -28% |
| `HeapByteBuffer` | 1,247 | 835 | -33% |
| `String` | 440 | 349 | -21% |
| `Object[]` | 655 | 480 | -27% |
| GC 总停顿 | 107.9 ms | 113.0 ms | 噪声内持平 |
| GC 最大停顿 | 8.4 ms | 7.3 ms | -13% |
| Full GC | 0 | 0 | — |

吞吐与正确性不变：20K TPS 稳定、`consumer_lag=0`、RingBuffer 65536、0 业务拒绝。
撮合路径 `Long` 装箱依旧为零（残余 112 条采样全部位于 Kafka 客户端内部）。

#### 4.7.4 过程教训

- `screen -X quit` 只结束 screen，**java 子进程可能残留为孤儿进程**。上轮引擎/sidecar 进程未清干净，
  与新一轮引擎同消费组/同 metrics 队列，导致首轮压测数据污染（lmax8 作废）；改为直接 `kill <pid>`
  确认清零后重跑（lmax9）。
- 压测开始时消费组重建会触发一次性 Kafka 重投递（本轮 17.5K 条重复单告警，集中在引擎启动后约 2 秒），
  去重逻辑正确处理、无业务影响；压测前应确认消费组稳定再开流。

JFR 样本：`~/match-jfr/lmax9.jfr`（本轮）/ `lmax7.jfr`（上轮基线）。

### 4.8 单实例 JVM 专项（120，20K TPS × 90s，出站发送移出撮合线程）

2026-08-24 第三轮验证。目标：LMAX 输出侧思路落地——把 SBE 编码 + Kafka 发送从撮合线程
移到独立发送线程，撮合线程（BLP）只做纯撮合业务。

#### 4.8.1 改造内容

1. `MatchOutboundSender` 重构为三路无锁 MPSC 队列（Agrona `ManyToOneConcurrentArrayQueue`）
   + 独立 `Outbound-Sender` 线程；
2. `flushEndOfBatch` 只把成交引用、深度快照数据、撤单确认移交队列后立即返回，不编码不发送；
3. 成交对象发送完成后由发送线程归还 `OrderBooks` trade 池（`ArrayDeque` → Agrona SPSC
   `OneToOneConcurrentArrayQueue`，发送线程 offer / 撮合线程 poll）；
4. 深度快照/撤单确认出站项池化（`OutboundDepth` / `CancelBatchItem`），撮合线程侧零分配；
5. 撤单确认从 `match()` 热路径即时编码发送改为 endOfBatch 批量移交，超 256 自动分批不丢数据；
6. 发送线程空闲时 `parkNanos(~1us)`，零流量低 CPU、恢复发送低延迟。

#### 4.8.2 压测结果（JFR 线程维度对比）

| 指标 | lmax9（上轮） | lmax10（本轮） | 变化 |
| --- | ---: | ---: | ---: |
| 撮合线程分配采样 | 2,631 | 1,607 | **-39%**（按 JFR 时长归一化 **-58%**） |
| 撮合线程 IoCodec/Kafka 编码帧采样 | 有 | **0** | 清零 |
| `HeapByteBuffer`（SBE 编码缓冲） | 371 | 0 | 清零 |
| `ProducerRecord` / `AppendCallbacks` | 100 / 104 | 0 / 0 | 清零 |
| `FutureRecordMetadata` 等 Kafka 内部对象 | ~200 | 0 | 清零 |
| `Long` 装箱 | 69 | 0 | 清零 |
| 残余 `byte[]`（全部为日志字符串拼接） | 619 | 326 | -47% |
| `Outbound-Sender` 线程分配采样 | — | 2,206 | 承担全部编码+发送 |
| GC 总停顿 | 113.0 ms | 114.1 ms | 持平（分配转移而非消失） |
| GC 最大停顿 | 7.3 ms | 8.39 ms | 噪声内持平 |
| Full GC | 0 | 0 | — |

吞吐与正确性不变：20K TPS、180 万单、0 发送失败、`consumer_lag=0`、RingBuffer 正常。

#### 4.8.3 结论

- 撮合线程彻底摆脱编码 + Kafka 发送（LMAX BLP 不碰 I/O 原则落地）：编码相关分配采样清零；
- 剩余撮合线程分配为订单簿遍历（`LongObjectSkipList` 迭代器/节点）与日志字符串拼接，
  是下一刀的目标；
- GC 总停顿持平是**预期**：这批工作只是转移线程而非消失，收益在撮合线程延迟抖动与
  Kafka 背压解耦，不在进程总 GC；
- 过程中修复两个问题：Spring 多构造器需显式 `@Autowired`；撤单确认批量项超容量会丢数据
  （改为循环分批 + 剩余前移保留，并补测试锁定）。

JFR 样本：`~/match-jfr/lmax10.jfr`（本轮）/ `lmax9.jfr`（上轮基线）。

### 4.9 单实例 JVM 专项（120，20K TPS × 90s，SBE 批量帧协议）

2026-08-24 第四轮验证。目标：阶段 2——多条成交 SBE 帧连续编码进一个 Kafka 消息
（`[uint32 帧长][SBE 帧]` 连续拼接），削减发送线程与消费端的 per-record 开销。

#### 4.9.1 改造内容

1. `IoCodec` 新增批量编解码：`encodeExchangeTradeBatch`（连续写缓冲，1 次精确拷贝 vs N 次）、
   `forEachExchangeTrade`（兼容旧单帧：首 4 字节 == OCX2 魔数直接按单帧解析）、`countExchangeTrades`；
2. `MatchOutboundSender`：发送线程每轮攒批（上限 128）→ 一次批量编码 + 一次 `producer.send`；
3. 消费端（push/spot/futures 三个 listener）全部改为批量循环解析；spot/futures 保持抛错重试/DLT
   语义（doMatch 幂等），push 逐笔容错；
4. 撮合线程 `submitTrades` 改索引遍历，消除 for-each 迭代器分配。

#### 4.9.2 压测结果（lmax11 批量帧 vs lmax10 单帧）

| 指标 | lmax10（单帧） | lmax11（批量帧） | 变化 |
| --- | ---: | ---: | ---: |
| 发送侧总分配字节（Outbound-Sender + producer 网络线程） | 1,582.8 MB | 706.3 MB | **-55%** |
| 按 JFR 时长归一化（170s→142s） | 9.31 MB/s | 4.97 MB/s | **-47%** |
| `HeapByteBuffer`（SBE 编码缓冲） | 280.2 MB | 45.4 MB | -84% |
| `ProducerRecord` | ~48 MB | ~13 采样 | -90% |
| `FutureRecordMetadata`/`AppendCallbacks`/`TopicPartition`/`RecordHeaders` | ~180 MB | 几乎消失 | 大幅下降 |
| GC 总停顿 | 114.1 ms | 99.9 ms | **-12%** |
| GC 次数 | 32 | 30 | -6% |
| GC 最大停顿 | 8.4 ms | 10.1 ms | 噪声内微升 |
| Full GC | 0 | 0 | — |

协议生效证据：本轮 `spot_exchange_trades_out_sbe` 仅新增 **28,331 条记录**（单帧模式约 140 万条），
即约 **49 笔成交/消息**；吞吐正确性不变（20K TPS、180 万单、0 失败、`consumer_lag=0`）。

#### 4.9.3 结论

- 阶段 2 是**分配削减**（与阶段 1 的分配转移性质不同）：发送侧分配 -55%，进程 GC 总停顿首次明显
  下降（-12%），最大停顿微升属单轮噪声；
- 批量帧同时降低消费端 per-record 开销与重投递消息数（128 上限控制单条消息爆炸半径）；
- 协议向后兼容旧单帧，消费端可先行升级灰度切换。

JFR 样本：`~/match-jfr/lmax11.jfr`（本轮）/ `lmax10.jfr`（上轮基线）。

### 4.10 单实例 JVM 专项（120，30K TPS × 180s，WAL 写入移出撮合线程 + 恢复 O(1) 索引）

2026-08-24 第五轮验证。目标：对齐 LMAX——Chronicle append 移出撮合线程（独立 `wal-write-*`
线程 + endOfBatch flush 屏障）；并顺带修复恢复路径的全订单簿线性扫描。

#### 4.10.1 WAL 线程化压测（30K TPS × 180s = 540 万单）

| 指标 | 结果 |
| --- | --- |
| 吞吐正确性 | 30,001 TPS、540 万单、0 失败、`consumer_lag=0` |
| 撮合线程 WAL append/Chronicle 帧采样 | 5 条（事件池回退 `new WalEvent`，池 256→1024 已修复） |
| `wal-write-*` 线程分配采样 | 全程仅 35 条——append 移出撮合线程且几乎零分配 |
| GC | 55 次、总停顿 315.9ms、最大 10.8ms、0 Full GC |
| 每百万单 GC 成本 | 55.5 → 58.5 ms/M（3 倍工作量下持平，无劣化） |
| 撮合线程分配（按百万单归一化） | 952 → 783 /M（-18%） |

WAL 编码（纯计算）保留在撮合线程，Chronicle append（I/O 性质）完全移出；批末 flush 屏障
保证本批事件全部 append，崩溃丢失窗口不劣于现状（fsync 本就异步）。

#### 4.10.2 恢复路径优化（O(1) 索引）

重启引擎验证恢复时发现：`applyMatchRecovery` 每次 MATCH 全订单簿线性扫描（O(订单数)），
540 万 MATCH 恢复 9.5 分钟未完成。改为恢复专用 `orderId → Order` 索引（Agrona
`Long2ObjectHashMap`，CREATE put / CANCEL remove / MATCH 两次 O(1) 定位），仅恢复期有效：

| 指标 | 线性扫描（旧） | O(1) 索引（新） |
| --- | ---: | ---: |
| 恢复耗时（748 万条 WAL） | 9.5 分钟未完成 | **约 11 秒（~100×）** |
| skipped | — | 0（WAL 完整无损） |
| 堆需求 | 1g OOM | **2g 稳定**（恢复大 WAL 需 ≥2g） |

引擎堆参数已由 `-Xmx1g` 放宽至 `-Xmx2g`，恢复内存需求同步写入运行手册与设计文档。

JFR 样本：`~/match-jfr/lmax12.jfr`（本轮）。

### 4.11 单实例 JVM 专项（120，40K TPS × 300s = 1200 万单）

2026-08-24 第六轮验证（清理 Kafka/WAL/Prometheus 后，sidecar 恢复运行）。

#### 4.11.1 JVM 表现

| 指标 | 结果 |
| --- | --- |
| 吞吐正确性 | 40,000 TPS 稳定 300s、1200 万单、0 失败、`consumer_lag=0` |
| GC | 90 次（74 G1New + 16 G1Old）、总停顿 725.4ms、最大 19.8ms、0 Full GC |
| 每百万单 GC 成本 | 60.4ms（20K→55.5、30K→58.5，随 TPS 缓升） |
| 每百万单分配采样 | 4,056 → 3,465（**-15%**，40K 下批量帧攒批效率更高） |
| 撮合线程分配 | 727/M（-7%），编码/Kafka/WAL append 均不在撮合线程 |
| 压测后 heap | 1.25GB（订单簿随量级线性增长；G1Old 16 次反映长生命周期对象压力） |

#### 4.11.2 过程中发现并修复的问题

1. **Grafana 无数据根因**：120 sidecar 未启动（Prometheus 抓取 down）。启动后链路恢复；
   注意 sidecar 只在启动时扫描一次 metrics 目录——**必须先启动引擎再启动 sidecar**，
   否则空目录下 sidecar 不会 tail 到后建的队列（本轮回放验证）。
2. **快照路径配置 bug**：`SnapshotService` 用 `spring.match.wal`（默认 `/data/match/wal`，
   120 无此目录）→ 改为 `RecoveryConfig`（`match.wal.data-dir`）。
3. **恢复堆积完全成交订单**：`ORDER_MATCH` 恢复只更新 executedQty 不移除完成订单 →
   1200 万单恢复 2g/3g/4g 堆相继 OOM。修复：恢复对齐运行时语义，完全成交订单 O(1)
   移出订单簿（Order 记录 mergeOrderRef + `MergeOrder.remove` 侵入式链表重链接）。
   1669 万条恢复 29s→17s（2g 堆），heap 3.7GB→1.57GB，skipped=0。
4. **快照编码 OOM（新发现）**：2g 堆下 `SnapshotCodec.encode` 序列化整个订单簿时 OOM
   （heap 已 1.7GB）。需流式/分块编码或按量级调堆，待办。

JFR 样本：`~/match-jfr/lmax13.jfr`（本轮）。

### 4.12 快照 mmap 化（Chronicle 队列流式快照）

2026-08-24 第七轮验证。目标：修复快照编码 OOM（4.11 发现）——`SnapshotCodec.encode` 一次性
构建完整 `byte[]` + 每订单 `new byte[1028]`，1200 万量级 2g 堆直接 OOM。

改造：

1. 快照格式改为 **Chronicle 队列（mmap）**：`snapshots/{symbol}/` 目录，第一条 = 元数据
   （walLastIndex/订单数），后续逐条订单 SBE；写入天然流式、零堆镜像、崩溃安全；
2. 逐订单**复用编码缓冲**（ThreadLocal），零堆分配；
3. **专用 snapshot-writer 线程**写盘（独立于 Spring scheduler），`@Scheduled` 只发起采集请求；
4. 恢复同构于 WAL 恢复：tailer 读元数据 → 流式 `addOrderDirect` → 从 `walLastIndex` 增量回放。

验证（120，40K×300s 灌单 + 500 TPS 触发快照安全门）：

| 指标 | 结果 |
| --- | --- |
| 快照写入 | **257 万挂单 → 620MB mmap 文件**，heap 峰值 1.08GB（修复前 2g 堆 OOM） |
| 快照恢复 | 加载 257 万订单 + 增量回放 8.3 万条，**启动 3.6s**（对比全量回放 17s） |
| 快照点一致性 | `walLastIndex` 写入/加载一致（88858595080374），`skipped=0` |
| 线程模型 | 写盘在 `snapshot-writer` 专用线程，撮合线程只做边界采集 |

注意：快照安全门（`isSnapshotSafe` = TPS<1000 && lag<1000）在高负载压测期间关闭（LMAX
"低活跃期快照"思想）；但纯空闲时无 `endOfBatch` 也无法采集——需低流量持续或强制采集机制，待办。

#### 4.12.1 快照采集请求事件（待办落地）

2026-08-24 修复"纯空闲无 endOfBatch 导致快照无法采集"：

- `DisruptorTemplate.requestSnapshot(symbol)`：向 RingBuffer 投递 `snapshotRequest` 事件；
- `OrderEventHandler.onEvent` 识别该事件：不撮合、批末 `endOfBatch` 触发采集；
- `SnapshotService.takeSnapshot`：`requestAll` 后逐 symbol 投递采集请求。

验证（120，引擎纯空闲无任何订单流量）：启动 65s 后快照自动生成（258 万订单、620MB、
`walLastIndex` 前进），107 tests 全绿。

### 4.13 长时压测 OOM + 订单簿内存标定（40K 持续 ~13 分钟 OOM）

2026-08-24 长时压测（40K×1800s 计划）在 **~13 分钟 OOM**——这正是长时验证的价值：

- heap 10 分钟 1857MB（2g 的 90%）→ 13 分钟 2047M 打满，GC 日志连续
  `Pause Full (G1 Compaction Pause) 2047M->2047M(2048M)` 400–550ms/次，消费积压 439 万，
  `OutOfMemoryError`；
- 根因：订单簿挂单线性增长（全是存活对象，G1 无法回收），无挂单上限保护。

订单簿内存标定（`--side 1` 纯挂单 + heap 线性拟合）：

| 指标 | 数值 |
| --- | ---: |
| 单订单内存 | ~340 B |
| 2g 堆最大挂单 | ~450–500 万 |
| 瞬时撮合吞吐（2g 堆 + 300–400 万挂单） | ≥80K TPS（40/60/80K 均 lag=0） |
| 40K 持续安全时长（2g 堆） | ~13 分钟 |

详见设计文档 §13.8。结论：任何固定堆都会在挂单达到容量上限时 OOM，**订单簿必须设挂单数上限**
（全局最大挂单数，按堆预算反推），否则无解。

### 4.14 全局挂单上限验证（20K×30 分钟 cap 拒绝 + 40K 混合深度压测）

实现全局挂单上限（`spring.match.max-open-orders`，默认 300 万；拒绝走 SBE `OrderReject`
出站 + `match_orders_rejected_total{reason="order_book_cap"}` 指标；恢复不拒绝、超限启动告警）
后的两轮验证：

#### 4.14.1 纯买单 cap 稳态（20K TPS × 30 分钟，`--side 1`）

| 指标 | 结果 |
| --- | ---: |
| 发送 / 实际 TPS / 失败 | 35,999,800 / 19,999 / 0 |
| book_size | 精确钉在 3,000,000，全程不越界 |
| cap 拒绝 | 32,971,852 笔 |
| heap / Old 区 | 锯齿 1.1–1.7GB / 稳定 1058 regions（~1.06GB），**无 OOM** |
| GC | 712 次 Young / **0 次 Full**，单次停顿 ~1.2ms |
| lag / VM 磁盘 | 0 / 稳定（topic retention.ms=600000，随跑随清） |

结论：**cap 是内存硬闸**——挂单顶满后新单全部拒绝，heap 收敛、GC 平稳，40K×13min 的
OOM 场景不再复现。

#### 4.14.2 随机买卖 + 真实深度模型（40K TPS × 300s，`--depth exp --tick 0.01`）

StressProducer 新增真实深度模型：指数衰减挂单距离（流动性向 mid 集中）、mid 随机游走
（均值回归），买在 mid 下方、卖在 mid 上方，价格按真实 tick（0.01）量化。

| 指标 | 结果 |
| --- | ---: |
| 发送 / 实际 TPS / 失败 | 11,998,200 / 39,994 / 0 |
| 成交 | 4,977,862 笔（~42% 订单参与成交，撮合+WAL MATCH+出站 trade 全路径） |
| 盘口形态 | 300 万挂单挤在 bid 8,905 + ask 8,782 ≈ 1.77 万价位（~170 单/档，真实形态） |
| cap | 约 2–3 分钟顶到 300 万，之后拒绝 3,677,888 笔（混合场景 cap 同样生效） |
| heap / GC | ~1.07GB / 累计 1.0s / **0 次 Full** |
| lag | 0 |

结论：40K 单实例单交易对在生产仿真负载（真实撮合+成交）下同样无压力；cap 在混合场景
仍精确兜底。观察：纯限价流（无 GTD 过期/用户撤单）净挂单必然增长到上限，cap 是最后防线，
生产还有 per-user limit 与订单生命周期清理兜在前面。

#### 4.14.3 压测暴露并整改：日志滚动（非限频）

30 分钟 cap 拒绝压测把 stdout 无滚动重定向文件撑到 10.7GB。按决策**做滚动配置而非限频**：

- 应用日志改走 logback 滚动文件（`LOG_PATH` 可配；`app.log` 50MB×7/500MB、
  `error.log` 20MB×14/200MB），stdout 重定向 `/dev/null`，runbook 同步更新；
- 修复 logback 双写 bug（`com.ocx` logger additivity + appender-ref 导致每条日志
  console/app 各打两遍）；
- 验证：拒绝 WARN 1:1 落盘（158 万行/158 万拒绝，orderId 无重复），app.log 按 50MB 滚动。

#### 4.14.4 引擎日志去 JSON（作用域限定撮合引擎，非全局）

JFR 实测拒绝洪峰（350K WARN/s）下 AsyncAppender 的 JSON 字符串构建占整机 ~28% CPU
（`String.encodeUTF8_UTF16`/`indexOf`/`ensureCapacity` 为栈顶），而引擎日志是人工
tail/grep 排障用途、仓库无任何日志采集器消费 JSON——纯属热路径开销。

处理原则：**只切撮合引擎，不动其它业务模块**。引擎启动新增
`-Dlogging.config=classpath:logback-match-engine.xml`（引擎专用纯文本配置，
`LOG_PATTERN_PLAIN` 无 JSON 转义/引号构建；滚动策略、脱敏 `%maskedMsg`、infra/error
通道与原配置一致）；`ocx-logging` 共享 JSON 配置保留给其它服务（有采集管道时 JSON 是
最佳实践）。实测 app.log 纯文本格式、JSON 行 0，cap/拒绝行为无回归。

> 注：AsyncAppender 是独立线程，去 JSON 降低的是整机 CPU 与 I/O（减少与引擎/消费者
> 的争抢），不直接抬升 300K 上限（该上限由消费者线程解码路径决定）。

### 4.15 单实例 350K 突破（120，350K TPS × 60s = 2100 万单，WAL 瓶颈专项）

**背景**：上一轮 350K TPS × 60s 实测积压 391 万、仅约 28.5 万/s 可持续，JFR 定位三个瓶颈：

- `MatchReconciler.checkLight` 每批执行，占撮合线程约 67% 采样（两次 volatile 读 + 三次
  `Decimal.compareTo` 分支，纯观测无状态）；
- `WalWriter.flush` 批末屏障每批全量等待 WAL 线程追平，撮合线程 park 约 59% 采样——
  WAL append 把撮合线程串行化；
- Chronicle `AbstractCloseable.threadSafetyCheck`（单线程守卫）占 wal-write 线程约 15% 采样。

**改动（2026-08-24，三处）**：

1. **checkLight 节流**：`match.reconcile-check-interval-ms=100`（默认），每 100ms 执行一次，
   纯观测无状态，正确性零影响；
2. **WAL flush 屏障节流**：`match.wal.flush-interval-ms=5`（默认），批末屏障每 5ms 才强制
   等 WAL 线程追平一次，其余批次直接放行（WAL 线程异步追赶）。语义变化：崩溃时最后
   ≤5ms×TPS 的事件可能尚未 append（事件队列 65536 封顶），由入站 Kafka + 快照兜底重建；
   快照采集/关闭路径改走 `flushNow()` 强制全量追平，快照记录的 lastIndex 与订单簿状态
   保持一致，恢复无重复应用；
3. **关闭 Chronicle 单线程守卫**：启动加 `-Ddisable.single.threaded.check=true`（库官方
   总开关，调用点 getstatic 守卫被 JIT 折叠为零成本）。前提：WAL 严格单写者
   （`wal-write-*` 线程独占 appender），契约见 `WalWriter` 类注释。

**压测结果（350K TPS × 60s，spot BTCUSDT，`--depth exp --tick 0.01 --side 0`）**：

| 指标 | 上一轮（opt3，改造前） | 本轮（改造后） |
| --- | --- | --- |
| 压测后积压 | 391 万 | **0（全部消化）** |
| 引擎处理订单 | ~1710 万（未跟上） | 20,820,475（全部） |
| 成交 | — | 14,467,867 |
| checkLight 采样（撮合线程栈顶） | 1662 | **0** |
| threadSafetyCheck 采样（WAL 线程栈顶） | 223 | **0** |
| WAL flush 屏障 park | 约 59% 采样 | 消失 |
| 撮合线程栈顶（稳态） | checkLight/flush 等待 | SkipList getOrCreate/randomLevel、outbound offer、Decimal.multiply、FIFO evict |
| GC | — | 36 次 young、0 Full，最大停顿 ~19.2ms（MaxGCPauseMillis=20） |
| 堆峰值 | — | ~1.84GB / 2GB（订单簿 3M 单占主） |

**结论**：350K 从"不可持续"变为"可稳定消费 + 处理 + 清积压"。WAL 的
"检查开销 + 屏障等待"被消除后，瓶颈转移到真实撮合工作与内存容量（订单簿 3M 单
顶满 cap，evict 生效）；下一档天花板预计由堆容量与 Kafka 单分区消费上限决定。

### 4.16 上限探测（120，400K TPS × 300s = 1.2 亿单，2G vs 4G 堆对比）

**目标**：在 350K 稳定之后，把压力与时长同时加大，找到当前配置（单实例单 symbol、
真实深度模型 exp、订单簿 3M cap）的处理上限，并验证瓶颈归属。

**第一轮（-Xmx2g，与 4.15 同堆配置）**：

| 指标 | 数值 |
| --- | --- |
| 洪峰期间引擎消费 | ~105.5M / 300s ≈ **35.2 万/s** |
| 压测结束积压 | 14,530,470 |
| 清积压耗时 | 31s（清积压速率 ~35 万/s） |
| Full GC | **54 次事件，累计 6.26s，单次最大 300ms** |
| 撮合线程 CPU | 65.3%（wal-write 55.2%、outbound 43%、G1 并发标记 13.3%×2） |
| JFR 撮合线程栈顶 | Long2LongHashMap get/put/compactChain、LongFifoSet.evictIfNeeded、SkipList randomLevel、logback MDC.remove |

结论：**400K 注入下稳态被 GC 卡在 ~35 万/s**——没有任何线程打满，但 G1 在 2G 堆 +
3M 满簿下频繁 Full GC（每 ~5s 一次、最长 300ms 停顿），直接把撮合线程按停。

**第二轮（-Xmx4g -Xms1g，其余同条件）**：

| 指标 | 数值 |
| --- | --- |
| 洪峰期间引擎消费 | ~114.8M / 300s ≈ **38.3 万/s**（末段实测 40.7 万/s） |
| 压测结束积压 | 5,164,403 |
| 清积压耗时 | <60s 归零 |
| Full GC | **0** |
| G1 并发标记 CPU | 13.3% → 6.0%（约减半） |
| 最大 Young 停顿 | ~54ms（4G 堆单次 evacuation 变大，略高于 2G 的 19ms） |
| JFR 撮合线程栈顶 | **TriggerOrderManager.updateTrailingStops（1086 采样，无条件单时每批空转）**、Long2LongHashMap、WaitUntilAppended、LongFifoSet.evictIfNeeded |

**结论**：上限瓶颈确认为 **GC（堆容量）**，不是撮合逻辑本身：

- 2G 堆 → 上限 ~35 万/s，Full GC 是主因；
- 4G 堆 → 上限抬到 ~38-40 万/s，Full GC 归零，此时瓶颈转移到真实撮合工作：
  `TriggerOrderManager.updateTrailingStops` 在**无条件单时每批仍无条件执行**（构造即预置
  5 个空 TreeMap，`checkStopOrders` 每批调用空转，占撮合线程 ~36% 采样）、
  Long2LongHashMap 查改、cap evict、SkipList 插入、WAL 批末屏障等待；
- 2G→4G 的代价是单次 Young 停顿从 ~19ms 升到 ~54ms（无 300ms 级 Full GC），
  对撮合引擎"宁可短停顿、不可长停摆"的取向更有利。

**下一个明确的优化点**：给 `checkStopOrders` 加条件单空检查守卫（TriggerOrderManager
维护条件单计数，为 0 时整段跳过），可消除约 36% 的撮合线程空转；压测场景（无条件单）
预期把 4G 堆的上限再往上抬。

### 4.17 条件单空检查守卫 + 450K×300s（4G 堆基线，1.35 亿单）

**改动**：`TriggerOrderManager` 维护未触发条件单计数（`orderCount`），暴露 O(1) 的
`isEmpty()`；`OrderEventHandler.checkStopOrders` 在无条件单时整段跳过——
400K 压测实测原先每批无条件执行 updateTrailingStops/checkAndTrigger 空 map 扫描，
占撮合线程约 36% 采样。addOrder/触发/取消三处同步维护计数（撮合线程单写，无锁）。

**压测（450K TPS × 300s = 1.35 亿单，-Xmx4g，同深度模型）**：

| 指标 | 数值 |
| --- | --- |
| 洪峰期间引擎消费 | ~118.3M / 300s ≈ **39.4 万/s**（末段实测 ~40.7 万/s） |
| 压测结束积压 | 16,654,748 |
| 清积压 | <75s 归零 |
| Full GC | **0** |
| 最大 Young 停顿 | ~36ms |
| JFR：updateTrailingStops | **1086 → 0**（守卫生效） |
| JFR 撮合线程栈顶 | flushEndOfBatch（679）、Long2LongHashMap get/compactChain、SkipList randomLevel、LongFifoSet.evictIfNeeded、Decimal.compareTo、OrderPool.release（CLQ.offer） |

**结论**：守卫生效（空转热点清零），4G 堆下引擎稳态上限约 **40 万/s**。与 4.16 的
400K 注入（末段 ~40.7 万/s）完全一致——说明 40 万/s 是当前测试拓扑下的稳定硬顶：
压测机=引擎机=Kafka 机（10 核 M4，Colima 占 6 vCPU），没有任何单个 JVM 线程打满
（撮合 54%、消费者 24%、WAL 14%、出站 4% 的采样占比），瓶颈从代码热点转移到了
"引擎 + 压测端 + Kafka VM 同机争抢"的整机资源边界。

**剩余热点（下一步候选）**：

1. `OrderPool.release` 用 `ConcurrentLinkedQueue`（跨线程 poll/offer + 每次 offer 一个
   Node 分配）——450K 采样约 186；可换 Agrona `OneToOneConcurrentArrayQueue` 或批归还；
2. `Long2LongHashMap` get/put/compactChain（compactChain 说明负载因子偏高，可扩容/换
   hash 策略）；
3. `flushEndOfBatch` 聚合路径（drainTrades/submitTrades 等，679 采样）；
4. 若要继续探引擎真实上限，需把压测端（或 Kafka）移出 120，消除同机争抢后再测。

### 4.18 OrderPool 环形队列化 + 40K×30 分钟长稳（4G 基线，7200 万单）

**改动**：`OrderPool` 由 `ConcurrentLinkedQueue` 换为 Agrona
`ManyToOneConcurrentArrayQueue`（消费侧入站线程单消费者、生产侧撮合线程+消费线程
错误路径多生产者；固定容量环形缓冲、零逐单 Node 分配）。容量按 2 的幂向上取整；
满池释放语义不变（直接丢弃、不复位）。新增 OrderPool 单测 3 个（复用/满池丢弃/非 2 的幂）。

**长稳压测（40K TPS × 1800s = 7200 万单，spot BTCUSDT，exp 深度，订单簿 3M cap 顶满）**：

| 指标 | 数值 |
| --- | --- |
| 洪峰期间积压 | **全程 0（实时消化，无追赶）** |
| Full GC | **0** |
| Young 停顿次数/最大 | 217 次 / ~19.5ms（贴着 MaxGCPauseMillis=20） |
| 堆（Old） | 稳定 ~1.5GB / 4G，无爬升（13 分钟 1464M → 结束 1496M） |
| 日志 | 0 ERROR；WARN 仅启动/填簿期（撮合耗时 9-17ms、未知 symbol 跳过），稳态无告警 |
| JFR：OrderPool/CLQ 采样 | 450K 轮 186 → **16（30 分钟累计）** |
| JFR 撮合线程栈顶 | SkipList clearUpdate、Long2LongHashMap get/compactChain、TradePlate.findPrice、flushEndOfBatch、evictIfNeeded |

**结论**：OrderPool 换型生效（逐单 Node 分配/CAS 热点清除）。40K 长稳 30 分钟
**未暴露任何新问题**：实时消费零积压、无 Full GC、堆无泄漏迹象、订单簿在 3M cap 下
evict 持续工作正常。长稳验证通过，40K 对当前 4G 配置是"低负载巡航"档位。

### 4.19 引擎热路径分配清理（120，40K TPS × 600s = 2400 万单，4G 基线）

**改动（LMAX"热路径零分配"路线，聚焦撮合线程自有代码）**：

1. `MatchMetrics.recordOrder/recordDuration`：键从每次字符串拼接改为按 symbol
   一次性预建（order/duration 键数组），热路径只做数组索引——JFR 基线里
   `StringConcatHelper` 占撮合线程分配 ~20.8%；
2. `OrderBooks.addOrder` / `TriggerOrderManager` 的 `log.debug` 全部套上构造时缓存的
   `logDebug` 守卫——DEBUG 关闭时参数求值（含 `long→Long` 装箱）完全跳过
   （JFR 基线 ~11.1%）；
3. `PendingReject` 由 record 改为池化可变对象（OrderBooks 内置 ArrayDeque 池，
   上限 65536），cap 拒绝时复用、endOfBatch 值拷贝后归还——JFR 基线 ~11.4%；
4. `refreshBestPrices` 缓存 best 档位 key，只有真正变化才重建 Decimal
   （JFR 基线 `Decimal.of` ~2.6%）；
5. `LongObjectSkipList.EntryIterator` 加 `reset()`，OrderBooks 每方向持有可复用
   迭代器，`getCurrentLimitPriceIterator` 返回前 reset——JFR 基线
   EntryIterator 构造+reusableEntry 共 ~14.7%；
6. 消费侧 tick/step 按 symbol 缓存（首次计算后复用，消除每单 `Decimal.parse`）。

**压测（40K TPS × 600s = 2400 万单，exp 深度，订单簿 3M cap 顶满，
cap 拒绝 1358 万单——比 4.18 的 30 分钟轮拒绝量多 5.6 倍，负载更重）**：

| 指标 | 4.18（40K×30min，改前） | 4.19（40K×10min，改后） |
| --- | --- | --- |
| Match-Handler 分配（30 分钟口径） | 26.1 GB | **1.0 GB（-96%）** |
| Match-Handler 分配速率 | ~14.0 MB/s | **2.4 MB/s（-83%）** |
| 总分配速率（JFR 全线程） | 51.6 MB/s | ~42.0 MB/s（-19%，且拒单多 5.6 倍） |
| Young 停顿次数 | 217 | 113 |
| Young 平均停顿 | 9.8ms | 7.3ms（<5ms 占 60%） |
| Young p99 / 最大 | ~19ms / 19.1ms | 17.0ms / 17.4ms |
| Full GC | 0 | 0 |
| 吞吐 / 正确性 | 40,000 TPS | 39,999 TPS、0 发送失败、lag=0 |

**JFR 撮合线程残余分配（按占比）**：`Decimal.multiply` 21.6%、批处理
`ArrayList` 扩容/toArray 约 23%、`Decimal.add/subtract` 19%、`MergeOrder.iterator`
7.3%、`submitReject` 值拷贝 7.1%——均为撮合本身固有成本，四类人为分配点
（字符串拼接/装箱/PendingReject/迭代器）已全部消失。

**结论**：引擎侧热路径分配已清干净，撮合线程不再是 GC 压力贡献者（分配速率
2.4MB/s，相对 4.18 的 14MB/s 降 83%）。**剩余最大分配源是消费线程
（~23MB/s，占总量 55%）：Kafka 客户端解码（parseRecord/HeapByteBuffer/
DefaultRecord 合计 ~60%）+ SBE `Decimal.of`（~8%）+ `OrderPool.acquire`（~5%）
——即"消费侧 SBE flyweight 复用 + 绕开 Kafka byte[] 交付"（报告 §4.9 之后
搁置的第 1 点），是下一轮优化的明确方向。**

### 4.20 消费侧分配优化：readVar 读侧缓存 + Decimal 复用（120，40K TPS × 600s = 2400 万单，4G 基线）

**目标**：4.19 结论把下一档优化指向消费线程——Kafka 公共 API 的 per-record
分配不可动，但我们代码内的 `readVar`（每单 byte[]+String）与 SBE `Decimal.of`
（每单 Decimal 对象）可以消除。

**改动 1（A 级）：`IoCodec` 读侧字符串缓存**。`readVarCached` + `ReadStringCache`
（8 槽、轮转替换、先拷 64B 探针再逐字节比较，命中零分配），入站热路径的
`symbol`/`timeInForce` 改为缓存读——与写侧已有的 `SymbolBytes` 对称。单 symbol
流命中率 ~100%，timeInForce（GTC/IOC/FOK/POST_ONLY）有限取值多槽近全命中。
`createTime` 等每单唯一字段仍走原 `readVar`，避免污染缓存。

**改动 2（B 级）：`Decimal.of` 按 (mantissa, exponent) 有界复用**。消费侧解码、
WAL 恢复、盘口 best price、出站成交路径共用同一个不可变 Decimal 缓存，命中直接
复用对象，不产生分配。调参过程记录（合成负载的真实约束）：

- 初版 1024 槽在 40K×600s（0.01 tick × ±200 区间 ≈ 4 万个价格档位）下命中率仅
  ~2.5%，JFR 占比仍为 8.0%（与基线持平）——槽位必须覆盖全部价格档位才有意义；
- 扩到 2^18（262144）槽后价格档位全覆盖，实测命中率 ~90%+；qty 为连续随机值
  （0.01–0.11，8 位小数）时按 hash 均匀散列，两次价格出现之间被 qty 覆盖到同槽的
  概率极低，价格项不会被高基数 qty 流踢掉；
- 内存代价：2MB 引用数组 + 至多 26 万 Decimal 对象（~8MB），对 4G 堆可忽略。

**压测（同条件：单实例 spot/BTCUSDT、`--depth exp --tick 0.01`、`-Xms1g -Xmx4g`、
订单簿 3M cap 顶满，JFR 样本 `~/match-jfr/consume-side-40k600s-r2.jfr`）**：

| 指标 | 4.19（改前） | 4.20（改后） |
| --- | --- | --- |
| 发送 / 实际 TPS / 失败 | 24,000,000 / 39,999 / 0 | 24,000,000 / 40,000 / 0 |
| 成交 | — | 7,298,849 |
| cap 拒绝 | ~1358 万 | 1340.6 万 |
| consumer lag / RingBuffer | 0 / 65536 | 0 / 65536 |
| JFR 消费线程 `Decimal.of` 占比 | ~8%（7.9%） | **3.8%（-54%）** |
| JFR 消费线程 readVar/String 分配 | ~5–6% | 基本清零（String.<init> 残余 1.2%，来自其它路径） |
| Full GC | 0 | 0 |
| Young 停顿（平均/最大） | ~7.3ms / 17.4ms | ~9.9ms / 17.8ms |

**结论**：消费侧我们可控的分配（readVar String + `Decimal.of`）削减约 60%+；
Kafka 客户端 per-record 分配（`parseRecord`/`HeapByteBuffer.slice`/`duplicate`/
`DefaultRecord.readFrom` 合计仍约 60%）是公共 API 固有成本（KAFKA-12605 至今
Open，社区无解），本轮不动。剩余 qty 分配在合成负载下不可消除（每单唯一），
真实 step 量化市场中 qty 也聚集，收益会更大。彻底绕开 Kafka per-record 分配需
batch 级原生消费（Dekaf 方向，内部 API 读 raw batch），版本耦合 + offset/重平衡
风险高，社区 KAFKA-1895 已放弃——记录为长期观察项，不做。

#### 4.20.1 50K 补充验证（120，50K TPS × 600s = 3000 万单，4G 基线）

在 4.20 优化后的代码上验证更高档位，确认"分配随 TPS 线性放大、无异常拐点"。
同条件：单实例 spot/BTCUSDT、`--depth exp --tick 0.01`、`-Xms1g -Xmx4g`、
订单簿 3M cap 顶满，JFR 样本 `~/match-jfr/50k-600s.jfr`。

| 指标 | 4.20（40K） | 4.20.1（50K） |
| --- | --- | --- |
| 发送 / 实际 TPS / 失败 | 24,000,000 / 40,000 / 0 | 29,999,800 / 49,999 / 0 |
| 成交 | 7,298,849 | 8,190,937 |
| consumer lag / RingBuffer | 0 / 65536 | 0 / 65536 |
| 消费线程分配速率 | ~19.4 MB/s | ~23.6 MB/s（按 TPS 线性吻合） |
| 消费线程 `Decimal.of` 占比 | 3.8% | 4.6%（绝对值随 TPS 放大） |
| 撮合线程分配 | ~1.95 GB | ~1.97 GB（cap 稳态下不随 TPS 加压） |
| Full GC | 0 | 0 |
| Young 停顿（平均/最大） | ~9.9ms / 17.8ms | ~9.6ms / 18.1ms |

**结论**：50K 完全站稳——0 积压、0 Full GC、最大停顿 18.1ms 仍贴
`MaxGCPauseMillis=20` 以内。关键观察：消费线程分配随 TPS 线性放大（无拐点），
撮合线程分配在 cap 稳态下与 40K 持平（4.19/4.20 清理的收益），引擎仍有
大量余量（历史上限探测 4.16/4.17 稳态 ~38-40 万/s），下一档边界探测方向为
100K+。

#### 4.20.2 100K 边界探测（120，100K TPS × 600s = 6000 万单，4G 基线）

目标：按 4.20.1 结论上探 100K 档位，验证稳定性并观察"是否接近边界"的早期信号。
同条件：单实例 spot/BTCUSDT、`--depth exp --tick 0.01`、`-Xms1g -Xmx4g`、
订单簿 3M cap 顶满，JFR 样本 `~/match-jfr/100k-600s.jfr`。

| 指标 | 4.20.1（50K） | 4.20.2（100K） |
| --- | --- | --- |
| 发送 / 实际 TPS / 失败 | 29,999,800 / 49,999 / 0 | 59,999,400 / 99,999 / 0 |
| 成交 | 8,190,937 | 11,743,952 |
| cap 拒绝 | — | 4,502 万（~75%，3M 簿顶满） |
| consumer lag / RingBuffer | 0 / 65536 | 0 / 65536 |
| 消费线程分配速率 | ~23.6 MB/s | ~45.5 MB/s（严格随 TPS 线性，无拐点） |
| 消费线程 `Decimal.of` 占比 | 4.6% | 6.0%（绝对值 725→1762 MB，随 TPS 放大） |
| 撮合线程分配 | ~1.97 GB | ~3.77 GB（50K→100K 约 2 倍，开始随量级放大） |
| Full GC | 0 | 0 |
| Young 停顿（平均/最大） | ~9.6ms / 18.1ms | ~9.9ms / 16.8ms |

**结论**：100K 完全站稳——0 积压、0 Full GC、最大停顿 16.8ms 仍在
`MaxGCPauseMillis=20` 内，仅为实测硬顶（4.16/4.17 稳态 ~38-40 万/s）的 1/4，
**不是边界**。早期信号：撮合线程分配 50K→100K 翻倍（cap 稳态下成交量/evict
频率随量级放大），消费线程分配严格线性。下一档边界探测建议 **200K×300s 快探**
（观察 lag 是否开始累积、Full GC 是否出现）。

#### 4.20.3 订单簿容量 3M→4M 对比验证（120，100K TPS × 600s，4G 基线）

目标：验证把 `spring.match.max-open-orders` 从 300 万提到 400 万的实际效果与代价。
同条件：单实例 spot/BTCUSDT、`--depth exp --tick 0.01`、`-Xms1g -Xmx4g`，
JFR 样本 `~/match-jfr/100k-600s-cap4m.jfr`。

| 指标 | 3M cap | 4M cap |
| --- | --- | --- |
| 发送 / 实际 TPS / 失败 | 59,999,400 / 99,999 / 0 | 59,999,100 / 99,998 / 0 |
| 成交 | 11,743,952 | **13,434,515（+14%）** |
| cap 拒绝率 | 75.3% | **70.8%（-4.5pp）** |
| consumer lag / RingBuffer | 0 / 65536 | 0 / 65536 |
| heap 峰值（GCHeapSummary Before GC） | 1,038 MiB | 1,332 MiB（+28%，4G 堆余量巨大） |
| Full GC | 0 | 0 |
| Young 停顿（平均/最大） | ~9.9ms / 16.8ms | ~9.9ms / 18.9ms |
| 撮合线程分配 | 3,774 MB | **5,903 MB（+56%）** |
| 消费线程分配 | 29,375 MB | 29,511 MB（持平） |

**撮合线程 +56% 的根源**：`MergeOrder.iterator` 1,058→2,363 MB（翻倍，占撮合线程
分配 40%）——订单簿越深，合并订单链表遍历（evict、撮合扫描）工作量线性放大；
`Decimal.multiply` 544→608 MB（+12%）、`submitReject` 447→384 MB（拒绝减少而下降）。

**结论**：4M cap 在 4G 堆上安全（heap 峰值 1.33GB、0 Full GC），业务收益明确
（成交 +14%、拒绝 -4.5pp），代价是撮合线程分配 +56% 与 Young 最大停顿略升
（16.8→18.9ms）。厚簿场景 4M 划算；若生产堆提至 6G 配 4M cap 更稳。
**下一个明确的优化点**：`MergeOrder.iterator`（大簿遍历的固有成本），调研
LMAX/开源撮合引擎是否用无迭代器方案（待办）。

#### 4.20.4 MergeOrder 迭代器可复用化（120，100K TPS × 600s，4M cap，4G 基线）

**调研结论**：开源撮合引擎已消除"每档位 new 迭代器"——exchange-core Fast 版用
环形数组 + 下标指针遍历（零迭代器对象），coralme 用预创建的可复用
`ReusableOrderIterator`（reset 复用、零分配）。两者殊途同归：热路径迭代器分配归零。

**改造**：`MergeOrder.iterator()` 由每次 `new` 匿名 Iterator 改为预建 `OrderIterator`
实例 reset 复用（coralme 模式）。同档位遍历无嵌套、撮合线程单写者，调用点零改动，
remove 语义与旧实现完全等价（O(1) 重链接 + onOrderRemoved 全局计数）。

**压测（同条件：4M cap、100K×600s、exp 深度、-Xms1g -Xmx4g，JFR 样本
`~/match-jfr/100k-600s-cap4m-iteropt.jfr`）**：

| 指标 | 4.20.3（改前） | 4.20.4（改后） |
| --- | --- | --- |
| 发送 / 实际 TPS / 失败 | 59,999,100 / 99,998 / 0 | 59,999,000 / 99,998 / 0 |
| 成交 | 13,434,515 | 13,607,103 |
| consumer lag / RingBuffer | 0 / 65536 | 0 / 65536 |
| 撮合线程 `MergeOrder.iterator` 分配 | 2,363 MB（40.0%） | **0** |
| 撮合线程分配 | 5,903 MB | **3,628 MB（-38.5%）** |
| Full GC | 0 | 0 |
| Young 停顿（最大） | 18.9ms | 17.7ms |
| heap 峰值 | 1,332 MiB | 1,330 MiB（持平） |

**结论**：迭代器分配清零后，4M 簿的撮合线程分配（3.63GB）反而比 3M 簿改前
（3.77GB）还低——厚簿的"遍历成本"被完全消除，容量优势无损保留。撮合线程
残余分配为撮合本身固有成本（Arrays.copyOf/Decimal.multiply/ArrayList.grow 等）。

#### 4.20.5 200K 边界探测 + 批处理列表零拷贝（120，200K TPS × 600s = 1.2 亿单，5M cap，4G 基线）

**背景**：4.20.4 结论撮合线程第一大分配源为 `Arrays.copyOf`（1,040MB，28.7%）。
JFR 调用链定位：`addPendingTrades`/`addPendingCompletedOrders` 的 `addAll`（先
toArray 整表再 System.arraycopy）+ 三个 ArrayList 反复扩容。改造：`addAll` →
`ensureCapacity` 一次到位 + 循环 add；撮合服务中间列表预分配容量。

**200K×600s（5M cap）第一轮暴露一个自引入回归**：`drainTrades`/`drainCompletedOrders`
预分配 128/64 容量在 200K 高频小批下是纯浪费——`ArrayList.<init>` 占撮合线程分配
**57.7%（7,986MB）**，Young 最大停顿升到 **62.9ms**。修复：改回空初始（共享空数组），
按需扩容由 `addPendingTrades` 的 ensureCapacity 一次到位。**教训：列表预分配要看真实
批大小，高频小批场景空初始 + 按需扩容优于盲预分配。**

**修复后压测（同条件，JFR 样本 `~/match-jfr/200k-600s-cap5m-r2.jfr`）**：

| 指标 | 100K/4M（4.20.4） | 200K/5M 修复前 | 200K/5M 修复后 |
| --- | --- | --- | --- |
| 发送 / 实际 TPS / 失败 | 60M / 99,998 / 0 | 120M / 199,994 / 0 | 120M / 199,996 / 0 |
| 成交 | 13,607,103 | 20,798,167 | 20,962,119 |
| consumer lag / RingBuffer | 0 / 65536 | 0 / 65536 | 0 / 65536 |
| 撮合线程分配 | 3.63 GB | 13.83 GB | **6.26 GB（-55%）** |
| 其中 `ArrayList.<init>` | — | 7,986 MB（57.7%） | **0（已消除）** |
| 其中 `Arrays.copyOf` | 1,040 MB | 980 MB | 1,301 MB（toArray 部分已消） |
| Full GC | 0 | 0 | 0 |
| Young 停顿（最大） | 17.7ms | 62.9ms | **18.7ms** |
| heap 峰值 | 1,330 MiB | 1,663 MiB | 1,674 MiB |

**边界结论**：**200K 完全站稳**——1.2 亿单、0 积压、0 Full GC、Young 最大 18.7ms
仍贴 `MaxGCPauseMillis=20` 内。5M cap 下 heap 峰值 1.67GB，4G 堆余量充足。
撮合线程残余分配：`Arrays.copyOf` 1,301MB（20.8%，ensureCapacity 扩容 + 编码路径）、
`submitReject` 1,006MB（16.1%，拒绝洪峰 9,400 万的价值拷贝）、`Decimal.multiply`
987MB（15.8%）。距 4.16/4.17 稳态硬顶（~38-40 万/s）仍有约 2 倍余量；
下一档边界探测为 300K。

#### 4.20.6 边界快探：300K/350K + 拒绝对象池扩容（120，5M cap，4G 基线）

**改动**：`submitReject` 的 `RejectItem` 对象池由 64 扩到 65536（与拒绝队列同量级）。
64 个池在 cap 顶满（>20 万/s 拒绝）下瞬间耗尽，撮合线程被迫每单 `new RejectItem`
（300K 实测占撮合线程分配 22%）；池化对象常驻 ~2.6MB，可忽略。

**300K×300s（9,000 万单）**：

| 指标 | 结果 |
| --- | --- |
| 发送 / 实际 TPS / 失败 | 89,999,200 / 299,997 / 0 |
| consumer lag（稳态） | ~1,400 恒定（实时消化），结束后归零 |
| Full GC / Young 最大 | 0 / 19.2ms |
| 撮合线程分配 | 5.0 GB/300s（`submitReject` 22.0% 为最大源之一） |

**350K×300s（1.05 亿单，池扩容后）**：

| 指标 | 结果 |
| --- | --- |
| 发送 / 实际 TPS / 失败 | 104,992,100 / 349,973 / 0 |
| consumer lag | 1,544 → ~2,000+（**缓慢累积**），注入停止后归零 |
| 吞吐瞬时 | 332.7K~350K（波动，消费开始跟不上注入） |
| Full GC / Young 最大 | 0 / 21.5ms |
| 撮合线程分配 | 4.49 GB/300s（**submitReject 掉出 top8**，池扩容生效） |
| 撮合线程 top：`Arrays.copyOf` 1,399MB（31.1%）、`Decimal.multiply` 883MB（19.7%） | — |

**边界结论**：稳态线位于 **300K~350K 之间，约 330-340K**——300K 实时消化，
350K 已开始 lag 缓慢累积（吞吐瞬时跌到 332K）。这是首轮"贴线"数据，与
4.16/4.17 的 400K 积压结论吻合（优化后边界略上移）。**0 Full GC 全程**，
Young 最大 21.5ms（350K 首次单次略超 20ms 目标）。下一档若需精确二分：
330K×300s。

## 5. 根因：CPU 争抢

105 开发机是 **2 物理核 / 4 线程**（i5-5200U）。4 个引擎分别 `--cpuset-cpus 0/1/2/3`，即 4 个线程挤在 2 个物理核上。

- 单实例：一个引擎独占约 1 个核，可持续消费约 10~14K TPS，撮合核心跟得上。
- 四实例并行：4 个 Disruptor 撮合线程 + 消费线程 + WAL 线程互相抢 2 个物理核，单实例吞吐掉到约 2K TPS，撮合核心来不及消费 RingBuffer。

结论：生产环境必须按设计文档执行“每个撮合实例独占一个物理核”；当前 cpuset 0/1/2/3 只是逻辑核，做不到真正隔离。

## 6. 结论

- 可观测性改造达到目标：headless + 推指标 + 过载降级 + 存活/过载告警，全链路可用。
- 单实例高负载下引擎正常工作（无崩溃、无误拒绝、消费端是瓶颈）。
- 多实例并行下 105 暴露的是**硬件资源**问题（CPU 争抢）；迁移到 120 后 40K TPS 总吞吐无过载，验证代码不是瓶颈。
- 热路径日志去装箱 + `ExchangeTrade.time` primitive 化后，GC 总停顿 269ms→108ms、最大停顿 13.3ms→8.4ms，
  撮合热路径零 `Long` 装箱（见 4.6）；日志/包装类型清理是可量化的 GC 收益来源。
- 出站批量路径免拷贝 + 编码侧缓冲复用后，JFR 分配采样再降 29.5%（10,738→7,565），最大 GC 停顿 8.4ms→7.3ms
  （见 4.7）；**KafkaProducer 标准 API 下出站零拷贝不可行**，进一步降低出站分配需将发送移出撮合线程
  （LMAX 输出 disruptor 思路），而非在 producer 层做文章。
- 出站发送移出撮合线程后，撮合线程分配采样再降 39%（按秒归一化 58%），编码/Kafka 帧采样清零
  （见 4.8）；GC 总停顿持平是分配转移的必然结果，收益在撮合线程延迟抖动与背压解耦。
- SBE 批量帧协议后，发送侧分配字节 -55%、GC 总停顿 -12%（见 4.9）；这是首次真正削减进程总分配，
  而非转移线程。
- WAL 专项三连（checkLight 节流 + flush 屏障节流 + 关 Chronicle 单线程守卫，见 4.15）：
  350K TPS × 60s 从积压 391 万变为 0 积压全消化；JFR 三热点（checkLight 1662、
  threadSafetyCheck 223、flush park）全部清零。持久性语义以"最后 ≤5ms 批次可能晚落盘"
  换取撮合线程不再被 WAL 串行化，快照/关闭路径 flushNow 保一致。
- 消费侧分配优化（4.20/4.20.1/4.20.2）：readVar 字符串缓存 + `Decimal.of` 复用使
  可控分配削减 60%+；40K/50K/100K 三档均 0 积压、0 Full GC，消费线程分配严格随
  TPS 线性放大，100K 仅为实测硬顶（~38-40 万/s）的 1/4——边界在更高量级。

## 7. 订单模型与 JVM 参数基线固化（2026-08-25）

### 7.1 现实订单模型（默认 exp 指数深度）

压测订单构造默认从 `uniform`（±spread 均匀平铺）改为 **`exp` 指数深度 + tick 量化**
（`--depth exp --tick 0.01`），模仿现实订单簿形态：mid 随机游走 + 均值回归、
订单距离指数衰减（流动性向最优价集中）、价格按 tick 离散、同价位聚集多单。
`uniform` 保留仅供订单簿内存标定（平铺场景）使用。

500 万在簿形态实测对比（BTCUSDT，price 70000 ±200）：

| 形态指标 | uniform | exp + tick 0.01 |
| --- | ---: | ---: |
| 价格档位 | ~40,000 | ~14,886（-63%） |
| 每档平均订单数 | ~125 | ~336 |
| 流动性 | 均匀平铺 | 近 mid 集中、随距离衰减 |

固化位置：`StressProducer` 默认 `depth=exp`、`stress-test.sh` 显式传
`--depth exp --tick 0.01`。

### 7.2 JVM 年轻代基线（-Xmn1536m）

500K TPS × 5M 在簿 × exp 订单模型，JFR 150s 三组对比：

| 指标 | 默认年轻代 | `-Xmn2g` | **`-Xmn1536m`** |
| --- | ---: | ---: | ---: |
| Young GC 次数 | 78 | 71 | 50 |
| 晋升老年代（OutsidePLAB） | 107,708 | 4,774 | 6,239 |
| G1Old mixed 周期 | 14 | 10 | 7 |
| Full GC | 0 | 0 | 0 |
| 单次 p99 | 24.1ms | 199ms（该轮长尾） | **38.1ms** |
| 单次 max | 29.4ms | 219ms | **54.6ms** |
| STW 总时长 | 1,084ms | 1,190ms | **899ms** |

结论：`-Xmn1536m` 是"长稳（晋升较默认 -94%，老年代零 Full GC）+ 延迟稳定
（无 200ms 级长暂停尖峰）+ 总 STW 最低"的折中基线，已固化到运行手册
`COMMON_JVM`（`-Xms1g -Xmx4g -Xmn1536m`）。

## 8. 待办

- 并行压测脚本（4 个 StressProducer 并发）可进一步固化到 `stress-test.sh`。
- Grafana 面板已改为 profile+symbol，但部分静态标题（BTC/ETH 文案）后续可再优化。
- 生产部署时确认每个引擎实例绑定独立物理核。
- 阶段 2（批量帧协议）已完成（见 4.9）：多条成交 SBE 帧连续编码进可复用大缓冲，
  消费端循环解析，GC 总停顿首次下降。
- 消费侧 SBE flyweight 复用已落地（见 4.20）：readVar 字符串缓存 + `Decimal.of`
  复用，可控分配削减 60%+；绕开 Kafka per-record 分配的 batch 级原生消费
  （Dekaf 方向）为长期观察项，暂不做。
- **收单路径改造（✅ 已实施，2026-08-28）**：订单入口收单从 Kafka 迁到 Aeron UDP
  （客户端/压测端直发 ingress），详见 match-engine-*.md 相关章节 与本报告 §18；
  同日出站也迁到 Aeron Archive（撮合链路完全脱离 Kafka），详见 §19。

## 18. 收单 Aeron 化改造（2026-08-28）

### 18.1 背景

§17 证明链路成本占完整引擎 ~92%（60 万 vs 纯撮合 755 万单/s）；主流交易所收单主路径
（Order Gateway → Matching Engine）用 Aeron，Kafka 只在撮合后做事件分发。我们把
**Kafka 收单**（ingress 从 `{profile}_in.{sym}` 拉订单）整体替换为 **Aeron UDP 收单**。

### 18.2 改动摘要

- ingress：`KafkaConsumerRunner` → `AeronOrderInRunner`（UDP 订阅轮询，零拷贝）；
  `MessageConsumerListener` 改 DirectBuffer 直读（去掉 Kafka 帧 memcpy）；
- 收单端点：`-Docx.ingress.aeron.udp.endpoint=0.0.0.0:11000`（默认），每 symbol 一个
  UDP 流（`StreamIds.inbound(symbol)`）；
- 删除 Kafka 消费配置（topic-pattern / consumer-group / fetch-max-records）；
- `match_consumer_lag` 语义迁移为入站 IPC 环积压（发布位置 − 订阅位置）；
- 出站同日跟进 Archive 化（§19）：撮合链路完全脱离 Kafka；
- StressProducer 改 Aeron UDP 发布（`--aeron-endpoint`），`stress-test.sh --big` 同步。

### 18.3 冒烟验证（三进程，121 远程 producer → 120 引擎）

1 × 10K TPS × 30s + 3s 预灌：producer 10,016 TPS、**0 错误、0 拒绝、lag=0**、
book=55,290、trades=38,616；生产端 offer 背压退避仅在启动建连期出现（~1 万次），
稳态零退避。单测含 UDP 收单端到端（独立双嵌入式驱动跨 UDP 直发 → 校验 → 解码 →
IPC 环 → core 直读）。

### 18.4 影响与后续

- 收单路径去掉 Kafka 一跳后，链路成本只剩 ingress 解码 + IPC + WAL + 出站；
  完整引擎上限的下一步瓶颈需重新压测标定（600K/700K 阶梯待重测）；
- 生产对接：backend 交易服务作为 Aeron 客户端接入收单端点（鉴权/风控前置）；
- 多发布者保序：当前语义为每 symbol 单发布者，多发布者由网关层定序。

## 19. 出站 Archive 化改造（2026-08-28，Bitvavo cold-log 模式）

### 19.1 背景

600K 压测两轮（三进程、5M 簿、90s、6 远程 producer 121→120）JFR 铁证：**瓶颈不在
UDP/ingress/core，在 egress→Kafka 出站**——egress 线程 7162/7237 采样（~99%）集中在
`tradeQueue.offer` 自旋（队列满），Kafka 发送线程全程仅 11 采样（`acks=all` + 跨机
121 RTT ~1ms + 默认 16KB/32MB）；阻塞链一路堵回 core/ingress/UDP。行业调研（Bitvavo
Spring I/O 2024 / LMAX）：撮合热路径不碰 Kafka，输出先落 Aeron Archive 持久化日志，
冷读端再慢慢喂下游。决策：**match 域不保留 Kafka，Archive 即出站最终出口**。

### 19.2 改动摘要

- 新增独立 `OcxArchiveMain`（第 4 进程），连接共享媒体驱动，记录 egress 发布的
  成交/深度/DLT 流（`StreamIds.archiveTrade/archiveDepth/ARCHIVE_DLT_STREAM`）；
- `OutboundSender`：KafkaProducer → 每 symbol `Publication.offer`（IPC），保留
  值池/无锁队列/批量帧（`[len][frame]`）；offer 背压自旋不丢数据，新增
  `match_outbound_published_bytes_total` / `match_egress_backpressure_total`；
- ingress DLT：`IngressDeadLetterSink` → `AeronDeadLetterSink`（Archive DLT 流）；删除
  `KafkaHealthTracker`/`RawTopicSink`/`kafka-clients`/`spring.kafka.bootstrap-servers`；
- `stress-test.sh --big` 五进程编排（媒体驱动 → Archive → egress → core → ingress），
  停 egress 后自动跑 `ArchiveProbe` 落盘验证（`--archive-count-trades` 回放统计成交帧数）；
- `ArchiveProbe`：列出 recording（stream/字节/位置），可回放成交流按帧计数。

### 19.3 冒烟验证（五进程，本地 producer → 120 引擎）

20K TPS × 30s（2×10K + 5s 预灌，exp 双边、撤单流、WAL 开启）：

- 链路健康：**0 错误、0 拒绝、lag=0、overloaded=0**，成交 112,835 笔；
- Archive 落盘：成交流 11,027,200 bytes（回放 115,313 帧）、深度流 653,536 bytes、
  DLT 流 0（无死信）；回放帧数 ≥ Prometheus 成交总数（差值 = 5s 指标快照滞后，
  无丢数据迹象）；
- 单测：全量通过（含出站 offer 编码/背压重试、DLT Aeron 发布路径）。

### 19.4 影响与后续

- 撮合链路彻底脱离 Kafka：入站 Aeron UDP、出站 Aeron Archive，链路成本不再含
  Kafka RTT/acks 阻塞；600K 以上阶梯需重测标定；
- **后续（不在 match 域内）**：冷读端（Bitvavo cold sink 等价物）从 Archive
  `replay` 回放成交/深度/DLT，按需喂 Kafka/MySQL/清算；`ArchiveProbe` 已示范同一套
  回放 API；recording 生命周期管理与 Archive 副本（replication）另行设计。

## 20. 单进程池子注入与全链路上限探测（2026-08-28）

### 20.1 producer 改造（Rust 单进程 + 订单池）

- **单进程**：压测注入端只起一个 producer 进程（多进程是 Kafka 时代历史遗留；
  Rust 版单进程可跑任意 TPS，100K ≈ 4% CPU）；
- **订单池预生成**：引擎启动前先灌满内存池（默认 `--pool-secs 30` = 目标 TPS × 30s，
  600K → 1800 万帧 ≈ 2GB；内存不紧张时可放大，探测上限时随 TPS 自动放大）；
  打印 `POOL_READY` 后等待 go 信号（引擎就绪）再重放；
- **热路径只改 ID**：重放时每帧仅写 8 字节 orderId（撤单再写 8 字节目标单号），
  零生成/零 RNG/零编码/零分配；价格/数量/方向/类型全为池内预生成的真实 exp 分布；
- `stress-test.sh --big` 流程：单 producer 预生成池 → 启引擎五进程 → go →
  连续注入（预灌+撮合一次跑完）；`--pool-secs`/`--heap`（core 默认 4g）可调；
- Java 版 `StressProducer` 已删除（`ocx-stress-producer` 为唯一注入端）。

### 20.2 压力阶梯（120 回环，五进程 + Archive，单 producer 池子）

| 目标 TPS | 注入结果 | 拒绝/积压/过载 | 备注 |
| --- | --- | --- | --- |
| 600K | 600,005 | 0/0/0 | ✅ 精确打满 |
| 700K | 700,005 | 0/0/0 | ✅ |
| 1M | 1,000,000 | 0/0/0 | ✅（池子 3000 万帧 ≈ 3.35GB） |
| 1.2M | 1,200,005 | 0/0/0 | ✅（pool-secs 15） |
| 1.5M | 1,500,000 | 0/0/0 | ✅（pool-secs 10） |
| 2M | 2,000,000 | 0/0/0 | ✅（1.9 亿帧 / 95s） |
| 2.5M | **2,183,402** | 0/0/0 | ⚠️ 打不满：core 单撮合线程 ~100%（JFR 99.6% 采样在 Match-Handler） |
| 3M | stall（~1.27-1.79 亿帧后冻结） | — | ❌ egress Aeron 客户端 conductor 饿死 10s 自杀 → 发送线程死等 |

### 20.3 3M 撞墙机制（已排除堆/GC/swap）

- 复现两次（core 6g / 4g 堆），egress.out 均为
  `ConductorServiceTimeoutException: service interval exceeded: timeout=10s, interval=10.8s`
  → `AgentTerminationException`：egress 的 Aeron 客户端 conductor 线程在 3M 峰值
  被饿死 >10s，客户端自我终止，所有 publication 关闭，发送线程对关闭的 publication
  无限 offer 重试 → 背压冻结整条链；
- 已排除：core 堆大小（4g 复现）、GC（egress 最大暂停 2.9ms、无 Full GC）、
  swap（稳定 407MB）；真因是峰值期线程调度饿死（忙等线程 + macOS 调度，待线程 dump 定论）；
- 连带暴露：producer offer 无超时 + 脚本 `wait` 无限等 → run 挂死；后续需加
  脚本级看门狗/offer 停滞超时。

### 20.4 2.5M JFR 热点（墙在 core 单线程 + WAL）

- **core**：3117 采样中 3106（99.6%）在 `Match-Handler-BTCUSDT`——
  `onOrderIn` 2188 / `onEvent` 1870 / `onFragment` 1850 / `match` 1061 /
  `OrderBook.cancel` 1034（70% 撤单率下撤单路径是大头）；
- **egress**：wal-write 线程占 65%（`WalWriter.append*` ≈3000 采样），
  `Outbound-Sender` 仅 7%——Archive 出站已不是瓶颈，WAL 持久化成为出站侧成本；
- **ingress**：`Unsafe.copyMemory` 974（入站解码拷贝）。

### 20.5 结论

- **单 symbol 单线程撮合的实用上限 ≈ 220 万消息/s**（含 70% 撤单，折合新订单
  撮合约 66 万笔/s）——core 单线程墙，背压从 core → 入站环 → ingress → UDP 一路传回；
- 对比 Wi-Fi 时代 164K：架构链路改造 + 回环 + 单进程池子注入提升约 13 倍；
- 突破方向（按收益）：① core 侧压撤单路径（`OrderBook.cancel` 的
  LevelSlots/Long2IntMap 操作）；② WAL 剥离出 egress 主进程；③ 多 symbol 分片
  （每 symbol 一个撮合线程，线性扩展）。

## 9. 500K 完整撮合长稳（2026-08-25，6G 堆 × 5M 在簿 × WAL）

在 §7 的"500 万在簿"基础上补完整撮合路径（side=0 真实撮合 + WAL）。完整架构结论
见设计文档 match-engine-*.md 相关章节；编排脚本 `stress-test.sh --big`
（多 Producer 并行 500K，见 §15.1）。

| 指标 | 结果 |
| --- | --- |
| 注入 | 灌簿 500,196 TPS（15s） + 撮合 499,998 TPS × 600s，合计 ~3 亿单，0 失败 |
| 在簿 | 5,000,000 全程顶满（cap 硬闸稳定） |
| 成交 / 拒绝 | 1921 万笔 / 3.35 亿笔 cap 拒绝 |
| 撮合延迟 P99 | 1–2 ms 全程 |
| 消化 / 积压 | 34.4–42.0 万/s；lag 末段 6108 万触发 overloaded（阈值 5000 万） |
| GC | 114 次 Young / 0 Full；稳态（>100s）平均 10.6ms、最大 68ms |
| 堆 | 稳态 ~3.2G / 6G（53%），0 OOM |
| WAL | 8.1GB（3 亿单 create+match） |
| JFR | 分配 top 全在 Kafka 客户端/协议边界；撮合线程分配仅 4%；CPU 采样撮合 45% / 消费 37% / 出站 10% |

**要点**：

1. 6G 堆对 500 万在簿为充分配置（稳态 53%），4G 堆 + 600 万 cap 的默认部署依旧成立；
2. 消化上限 ~40 万/s 是 Kafka 单分区单消费者硬边界，与 §4.16/4.17 的结论一致；
   500K 灌入必然积压，要消化需多分区多实例或消费侧批处理改造；
3. 500 万在簿下 P99 仍 1–2ms——订单簿规模与单笔撮合延迟解耦；

## 10. 净化环境复核、跨机拓扑对比与瓶颈定位（2026-08-25 晚）

### 10.1 环境净化与 500K 复核

净化：停 redis/mysql/jaeger/consul/cloudbeaver/arcane 六个容器、Colima VM 内存
11.65G→8G、清理 15GB 旧业务镜像。复核结果（见设计文档 §13.8.9）：

| 指标 | 竞争环境（§9） | 净化环境 |
| --- | --- | --- |
| 300K 注入 | — | lag 恒 ~1500（零积压）、P99 1ms |
| 500K × 600s 消化 | 34.4–42.0 万/s | **~41 万/s**（lag 匀速累积，末段 5571 万） |
| 稳态 GC | 10.6ms / max 68ms | **8.6ms / max 53.4ms** |
| P99 / Full GC / OOM | 1–2ms / 0 / 0 | 1–4ms / **0 / 0** |

净化释放的 CPU/内存**没有改变消化上限**（41 万/s 与竞争环境一致），只改善了 GC
停顿与延迟稳定性——证明上限由**单消费者保序结构**决定，而非资源竞争。

### 10.2 跨机拓扑实测（WiFi 链路，均不成立）

| 拓扑 | 实测 |
| --- | --- |
| Kafka 放 121，120 引擎跨机消费 | **2.4~3.3 万/s**（ConsumerProbe 23.5K/s 与引擎一致） |
| Kafka 放 105，120 引擎跨机消费 | **~1 万/s** |
| Producer 放 121，跨机注入 120 | **~5 万/s**（fill 2.7 万/s） |

网络：120↔121 RTT avg 8.6ms / max 65ms / 抖动 8.3ms；120↔105 RTT avg 18.9ms /
max 152ms / 抖动 32.5ms——均非亚毫秒稳定链路，TCP 拥塞窗口起不来。结论：WiFi 下
跨机（无论 Kafka 还是 producer 方向）不适合高吞吐压测，**本机净化拓扑是唯一可行方案**。

### 10.3 瓶颈定位（500K 长测 JFR，12,588 ExecutionSample）

- 撮合线程 48% / 消费线程 35% / 出站 9% / WAL 3%——两条串行单线程构成 41 万/s 上限；
- broker 本地纯 fetch 113 万/s，Broker/网络/内存/GC 均非瓶颈；
- 消费侧分配 top 全为 Kafka 客户端 per-record 对象（HeapByteBuffer 16K、
  ConsumerRecord 9K、DefaultRecord 8K、RecordHeaders 3.3K、Optional 2.3K）；
- **保序约束**：同 symbol 撮合须全局有序，多消费者并行不可行。

### 10.4 优化方向（待方案讨论）

1. 消费端 **Dekaf 批处理**（绕开 Kafka per-record 分配）→ 41 万 → 60~80 万/s 预期；
2. 撮合线程热点拆分（簿操作 / WAL flush / 自成交检查）；
3. `validateOrder` tick/step remainder 校验下沉。
4. JFR 验证撮合核心零分配（优化收敛），分配瓶颈在引擎外的 Kafka 客户端。

## 11. SBE 直通入站重构实测（2026-08-25 夜，分支 feat/sbe-direct-ingest）

### 11.1 动机与结论

§10.3 的"消费线程 35%"进一步拆栈：CPU 大头不是 Kafka 解析（~10%），而是我们自己的
字符串化解码层——`ReadStringCache.regionEquals` 48.6% + `Order.timeInForceCode` 14.1%，
根因是 schema 把 `symbol`/`timeInForce` 定义成 `varString`。结论：**编解码问题要在
编解码层根治，不转移热点**——schema 原生化 + 事件槽承载 SBE 字节 + 撮合线程原生直读，
`ReadStringCache`/`regionEquals`/`InboundFields` 整层删除。

### 11.2 实测数据（6G 堆 / 500K×120s / cap=5M / WAL，120 本机）

| 指标 | 直通前（§10） | 直通后 | 变化 |
| --- | --- | --- | --- |
| 消化上限 | ~41 万/s | **~50 万/s** | **+22%** |
| 消费线程采样 | 34.8% | **~10%** | 解码热点消除 |
| 撮合线程采样 | 48% | 63.5% | 唯一主瓶颈 |
| P99 | 1–4ms | **2–3ms** | — |
| Young GC / 120s | 112 次 | **25 次** | 分配压力大降 |
| Full GC / OOM | 0 / 0 | **0 / 0** | — |
| engine.out | — | 4.8GB → MB 级 | 耗时告警按秒节流 |

### 11.3 撮合线程新热点（63.5% 采样的构成，下一轮方向）

- `TradePlate.copyLevels`（深度快照发布）12.6%；
- `LongObjectSkipList.clearUpdate/getOrCreate`（价格树）11%；
- `LimitPriceMatchServiceImpl.match` 主逻辑 10%；
- `bookIndex.containsKey`（Long2ObjectHashMap）8.8%；
- `Unsafe.copyMemory`（memcpy/拷贝路径）5.7%。

### 11.4 "SBE 直达"的精确边界

传输层 SBE 直达已达成（字节零解析进事件槽、字段按偏移直读）；但撮合执行仍基于领域
对象 Order——SBE 帧是传输格式，Order 是撮合域内存态（resting 生命周期 / MergeOrder
侵入式链表 / bookIndex 引用），handler 从 SBE 直读原生字段后落进复用池 Order 再撮合，
该步骤为原生 setter 零分配，不是旧链路式的反序列化中间链。更激进"消灭 Order"的目标态
当前瓶颈不在字段读取，暂不推进（详见设计文档 §13.8.10）。

## 12. 吞吐第二阶段：撮合线程热点优化实测（2026-08-25 深夜）

### 12.1 改动

- 深度快照降频 200→500ms；`MergeOrder` 盘口数量聚合（add/remove/成交钩子 O(1) 维护，
  ensureFresh 重建 O(5M)→O(100)）；
- `bookIndex` 判重前移 cap 检查（满簿快路径）；
- `OrderRejectData` 原生化（发送线程每单 2 次 Decimal.of 清零）；
- 价格档缓存 1024 槽（getOrCreate 22.5%→0）；
- 撮合线程免重复 templateId 校验（10.3% getShort→0）。

### 12.2 实测（6G / 500K×120s / cap=5M / WAL）

| 指标 | §11 后 | 本阶段 |
| --- | --- | --- |
| 消化上限 | ~50 万/s | ~50 万/s |
| P99 | 2–3ms | **1ms** |
| ensureFresh/copyLevels | 12.6% | 0 |
| getOrCreate（价格树） | 22.5% | 0 |
| Outbound-Sender | 16.1% | 6.6% |
| Young GC / 120s | 25 次 | 27 次 |
| Full GC / OOM | 0 / 0 | 0 / 0 |

### 12.3 结论：瓶颈转为撮合线程整体处理率

内部热点全部归零后吞吐仍 ~50 万/s，撮合线程 62.6% 采样剩余构成：SBE 字段读取
（getShort 14.1%）、match 主逻辑（10.7%）、memcpy 检查（10.2%）、flush（5.2%）、
bookIndex put/containsKey（~10%）。下一阶段候选：WAL 批末屏障（wal-write 7.3% +
flush 5.2%，批间硬同步嫌疑）、撮合主逻辑本身、消费/出站节奏。

## 13. WAL 屏障与批节奏排除实验（2026-08-25 深夜）

为定位 50 万/s 上限，实施两组 A/B 对照（500K×120s，其余条件不变）：

| 实验 | 方法 | match TPS | 结论 |
| --- | --- | --- | --- |
| WAL 批末屏障 | `match.wal.flush-barrier-enabled=false`（flush() 直接放行，WAL 异步追赶） | ~50 万/s | **排除**：批末同步非瓶颈 |
| 消费批大小 | `match.consumer.fetch-max-records=50000`（默认 5000） | ~50 万/s | **排除**：消费批节奏非瓶颈 |

同时落地两个残余热点优化：`OrderEventHandler` 惰性缓存 MatchService（免每单
HashMap.get，原 4.3% 采样）、`IoCodec.readOrderIn` 常量 blockLength/version wrap
（免每单两次 getShort，原 10.7% 采样）。

**结论**：全部可识别热点清零、批间同步与批节奏排除后，50 万/s 为当前单实例单交易对
（5M 在簿、WAL、500K 灌入）的稳态上限——由撮合线程每单总成本（match 主逻辑 +
bookIndex 维护 + WAL 编码入队 + 出站提交 ≈ 2μs/单）决定。进一步突破需架构级方向：
多实例分区（单交易对保序约束）、撮合算法结构重构、或原生实现——均超出当前阶段。

## 14. 上限归因实验（2026-08-25 深夜）：排除内存/批/在簿规模

为回答"卡点是 CPU 还是内存"，追加三组对照（其余条件不变）：

| 实验 | 方法 | 结果 | 结论 |
| --- | --- | --- | --- |
| 在簿规模 | cap=5M → 1M（1M 满簿） | 吞吐不变 | **内存容量/工作集非瓶颈**（1M 与 5M 无差别，且零 OOM 零 Full GC） |
| 注入梯度 | 500K → 600K | lag 增长 30K/s → 130K/s | **真实上限 ≈ 47-50 万/s**：500K 注入下"match=50 万"是注入恰被消化的错觉，lag 增长才是真实缺口；600K 交叉验证一致 |
| 消费批 | fetch.max.records 5000 → 50000 | 600K 下无改善 | 消费批大小非瓶颈 |

**最终归因**：既不是 CPU 多核算力不足（撮合线程采样仅 62%，含等待），也不是内存
容量不足（1M/5M 无差别），而是**单线程保序串行的每单处理延迟**——约 2μs/单的固定
成本（match 主逻辑 18.3% + bookIndex 维护 + SBE 读取 + WAL 编码入队 + 出站提交）。
全部热点与外部环节排除后，该成本是当前 Java 单线程撮合数据结构的硬下限。

**突破方向（按代价排序）**：
1. 撮合主逻辑本身（18.3% 采样）：taker 吃单循环 / MergeOrder 迭代 / processMatch；
2. 每单必经路径压缩：validate/fillOrder/bookIndex put-remove/WAL 事件合并；
3. 架构级：按价格档分片并行撮合（保序约束内）、或原生实现核心（超出当前阶段）。

## 15. Aeron 三进程架构与订单混合压测（2026-08-26）

### 15.1 架构与验证基线

进程内 Disruptor 移除，改为三进程 + Aeron IPC（共享内存）：
`ingress（Kafka 消费/校验/SBE 解码）→ 入站环 → core（纵表撮合）→ 出站环 →
egress（WAL/Kafka 出站/metrics）`。撮合核心运行期零 Kafka、零 SBE 解码、零磁盘。
热路径优化：成交批次打包（TYPE_FILL_BATCH 单消息 ≤42 笔）、Long2IntMap 删除墓碑化、
ReusableFillList 逻辑清空、指标独立流（出站主流恢复严格 SPSC）。

**500K × 30 分钟长稳（5M 满簿，WAL 开）**：

| 指标 | 结果 |
| --- | --- |
| 订单 | 9 亿（500K TPS 全程达标） |
| 成交 | 3325 万笔 |
| 拒绝 | 8.33 亿（全为满簿 cap，测试模型无撤单所致——已通过撤单流修复） |
| lag | 全程 1-3.5K，不增长 |
| overloaded / Full GC | 0 / 0 |
| WAL | 3.8G（TEN_MINUTELY 滚动） |

### 15.2 压测机制改进（订单混合 + 撤单流）

原模型无撤单流 → 在簿数单调增长、开局灌满 cap 后 90%+ 单被拒绝（非稳态）。
改进：预灌 15s→5s（双边轻预热）+ `--cancel-ratio 0.7`（撤近期限价单，窗口 ≈2s@500K）
+ 订单混合（市价 10% / 条件单 2% / 冰山 5% / 大单 2% / 价格 regime）。
入站线格式新增 `visibleQty` 字段（冰山单可从 Kafka 进引擎）。

**500K × 90s 混合单**：rejected=0、簿子自然成长（~1.2M）、lag ~1K、0 Full GC、
市价单真实处理 120 万笔。期间挖出并修复两个条件单核心 bug：

- `TriggerSlots.checkAndTrigger` 扫描方向错误（BUY 触发价表降序，原逻辑从链头用
  `key<=lastPrice` 判断且不满足即 break——多档位永远扫不到低位触发档）；修复为
  `LevelSlots.firstLevelFrom` 从触发区间起点扫（O(log n + 触发数)）；
- `TriggerSlots.add` 触发价档位表耗尽抛异常导致订单静默丢弃；改为容量满返回
  `trigger_book_cap` 拒绝 + 档位容量 4096→65536。

### 15.3 800K 边界验证（10M 簿，90s）

| 指标 | 结果 |
| --- | --- |
| 注入 | 800,400 TPS（6 × 133,333，producer 达标） |
| 引擎处理 | ~59-60 万/s（lag 40s 后线性上涨，净积压增速 ~20 万/s） |
| 最终 lag | 956 万（真实掉队，非稳态） |
| 入站环积压 / overloaded / Full GC | 0 / 0 / 0（引擎内部健康） |
| book / rejected | 156 万（10M cap 未触及）/ 0 |

**结论**：撮合逻辑与内存均健康，瓶颈是本机（Apple M4，4 P-core）整条管线
（消费+解码+撮合+WAL+出站）的总 CPU 超过 4 核。稳态边界为 **500K TPS / 5M 簿**
（30 分钟长稳验证）；突破 800K 需更多核或多引擎实例并行。

## 16. 三进程常态化压测与热点优化实测（2026-08-27）

### 16.1 压测机制：三进程编排（替代单 JVM 装配根）

压测脚本 `--big` 改为四 JVM 编排（媒体驱动 + ingress + core + egress），每进程独立
JFR/GC 日志，倒序停机；`--no-affinity` 保留。**不再使用单 JVM 装配根**——单进程下 4 线程
合计易被误判为引擎能力。遗留进程清理覆盖全部主类；`--producer-host` 支持 producer 外移
（远程 classpath 走 `$HOME/stress`，密码参数化）。

### 16.2 三进程指标合流修复

三进程下 `match_consumer_lag` / `match_throughput_current` / `match_orders_rejected_total`
原先恒 0（指标生在不同进程未合流）。新增 ingress→core 指标流
（`TYPE_INGRESS_METRICS`，stream base 120000，2s 周期），core 合并后随 METRICS 行进
Grafana。修复后运行中实测：`match_throughput_current=15109`、`match_consumer_lag=297`
（此前恒 0）。

### 16.3 热点优化实测（每轮 500K×90s，JFR 对比）

| 轮次 | 优化 | 效果 |
| --- | --- | --- |
| 触发预检 | 条件单扫描 O(1) 边界预检 + 缓存扩容 | `checkAndTrigger` 29.9% → 0；撮合采样 1205 → 821 |
| egress symbol | 出站解码 symbol 预绑定 + 路由 long 比较 | egress `readAscii/String` 67.9% → 0；ingress 路由 10.8% → 0 |
| 撤单 level 列 | 订单槽位记录档位，摘除省 price→level 哈希 | `Long2IntMap.get` 22.6% → 11.3% |
| WAL 批处理 | 批末打包 + 子行去 symbol + 分片修复 + 事件池扩容 | 500K offer 12.3%→0、撮合采样 803→717；600K lag 2707→2143（-21%） |

### 16.4 边界验证（500K 长稳 / 600K / 700K）

**500K × 600s（两次，WAL 批处理前后各一）**：

| 指标 | 结果 |
| --- | --- |
| Producer | fill 500,400 / match 499,908-500,028 TPS 达标 |
| 成交 | 6681-6762 万 |
| lag | 全程 0.9-4.7K 波动、**不增长**（稳态特征） |
| 拒绝 / overloaded | 0 / 0（全程） |
| GC | 10 分钟仅 1-2 次 Young、0 Full |
| WAL | 8.3-8.4G（600s @500K） |

**600K × 90s（WAL 批量后）**：lag 2143 稳定、0 拒绝、0 过载——短测稳（无长稳验证）。

**700K × 90s（优化前后）**：lag 60s 12241 → 7261（优化后掉队斜率减半），仍从 1.3K 爬升至
7.3K——**临界，非稳态**。撮合线程 99.8% 满负荷，缺的是单线程余量。

### 16.5 producer 外移实验（105，2 物理核）

把 6 个 producer 全部移到 105（2 物理核）后：producer 只能发到 ~394K（目标 600K），注入
不足；394K 注入下撮合线程仍 98.5% 忙、lag 4215——**producer 抢核不是边界主因**，撮合线程
负载只跟注入成正比（采样 603/979 ≈ 394/600）。120 的瓶颈是引擎自身管线 + Kafka VM。

### 16.6 极限结论

本机（Apple M4，4P+6E，16GB）全链路极限：**500K 长稳稳态 / 600K 短测稳 / 700K 掉队**。
撮合线程 500K 即 99.8% 忙，每单 ~2μs 固定成本为硬下限；macOS 绑核无效（OpenHFT
`OSXJNAAffinity` 为空实现）。对标 LMAX 单线程 BLP 600 万单/秒（纯撮合），我们完整链路
50-60 万/s、纯撮合主逻辑折算约 250 万/s 量级——差距在链路成本（解码/WAL/出站/哈希维护），
不在芯片算力。突破方向：撮合核心进一步剥离链路（事件溯源 + 缓存预热）或原生核心，或
Linux 绑核 + 更多 P-core / 多引擎分片。

## 17. 纯撮合核心基准：绕过 ingress/egress 直测 match-core（2026-08-27）

### 17.1 动机与方法

完整链路 60 万/s 下无法区分"撮合逻辑成本"与"链路成本"。用 `CoreMatchBenchmark`
（`ocx-match-core/src/test/java/com/ocx/stress/`，与 StressProducer 并列）**绕过
ingress/egress/Aeron IPC/WAL**，直接构造 `OrderInFields` 喂 `OrderEventHandler.onEvent`
全流程（validate + 分派 + 撮合 + 条件单 + 冰山），单线程 90s 直测订单簿+撮合能力上限。

装配：BTCUSDT、5M 簿、TriggerSlots(cap/4, 65536)、本地计数 FillSink（零编码零发送）、
WAL null、自成交保护 false；订单模型复刻压测（exp 价格 / 70% 撤单 / 10% 市价 /
2% 条件单 / 5% 冰山 / 2% 大单）。

### 17.2 结果

| 模式 | TPS | 说明 |
| --- | --- | --- |
| 运行时生成（撤单 76%） | 922 万/s | Random/log 生成占 ~48% 采样（含生成开销） |
| **pregen 预生成回放** | **755 万/s** | 生成开销消除（残留 0.42%），簿稳态、高成交的真实撮合上限 |
| 完整链路（三进程） | 50-60 万/s | 500K 稳态 |

### 17.3 关键发现

1. **撮合核心单核真实上限 ≈ 755 万单/s**（超过 LMAX 公开 600 万/s 基准，M4 单核
   4.4GHz + 纵表 SoA）；运行时生成模式 922 万/s 说明撤单占比越高（撤单便宜）上限越高；
2. **链路成本占 ~92%**：完整链路 60 万 vs 纯撮合 755 万——每单的解码/IPC/WAL/出站/
   同机争抢吃掉绝大部分能力；瓶颈不在撮合逻辑；
3. **pregen 纯撮合热点**：matchTaker 25.7% / Long2IntMap.put 14.5% / findUpdate 10.2% /
   getOrCreate 8.3% / addOrderInternal 8.2%——撮合主体 + 哈希/档位维护；
4. **单线程观感问题**：10 核机器上单核满 = jvmUser 10%（JFR CPULoad），btop 每核都低
   是单线程核间迁移（macOS 调度）的显示效应，不是压力不足；main 线程 ThreadCPULoad
   9.88% ≈ jvmUser 10%，证明 JVM 全部 CPU 归撮合线程、单核满转；
5. **macOS 无法绑核**（OpenHFT OSXJNAAffinity 空实现），单线程核间迁移可能损失缓存
   局部性——Linux 绑核后单核上限或可更高（待验证）。

### 17.4 结论

match-core 的撮合核心架构优秀（755 万单/s、零 GC、探测 2.18 步），完整引擎的
性能天花板在链路成本而非撮合内核——后续突破方向：链路继续剥离（解码/WAL/出站移到
其他核/进程）或 Linux 绑核 + 更多 P-core。


