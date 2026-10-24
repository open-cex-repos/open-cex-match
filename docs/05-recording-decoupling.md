# 录制端解耦专项 — 诊断与第一轮修复（2026-09-01）

> **本章解决**：入站 journal 如何录制、录制如何从撮合线程解耦。
> **前置依赖**：01（Archive）、04（主备形态）。
> **读者前置**：Aeron Archive / recording。

> 迁移自 open-cex-docs `design-docs/19-recording-decoupling.md`（2026-09-05 迁入 open-cex-match/docs）。

## 1. 问题

全形态（入站录制 + 成交/深度/DLT recording + 快照全开）没有干净稳态平线：
瞬时 2–4s 锯齿摆动（0.4–0.9M 停滞 / 1.3–2.0M 爆发交替），std≈47 万，
自相关主峰 4s / 14s / 56s。

## 2. 诊断方法（新工具链）

| 工具 | 作用 |
| --- | --- |
| `RecorderLagProbe`（core） | 200ms 采样发布端位置（媒体驱动 pub-pos 计数器，按 label 匹配）vs Archive 录制位置（`getRecordingPosition`），输出滞后序列 |
| `perf-flatline-analyze.py` | producer 每 2s 采样 → TPS mean/std/p10/p90/自相关峰；与磁盘采样做交叉相关对齐 |
| `perf-disk-sample.sh` | 1s 节拍采样 iostat + Archive 段文件数/字节 |

## 3. 排除项（D1–D4 对照，单变量）

| 变量 | 结果 |
| --- | --- |
| GC | Archive/core/egress 均无实质停顿（egress 唯一 Young GC 0.9ms） |
| CPU 超订 | 全链路瞬时 CPU ≈ 405%（10 核），无饿死；Archive 仅 27% |
| 段文件 128MB→1GB | std 不变（0.47M），zero-progress 比例反升（0.17→0.26） |
| fileIoMaxLength 1MB→16MB | 无效 |
| 快照 10s/60s | 摆动依旧；60s 有独立 56s 自相关峰（对齐 purge） |
| 入站行 264B→实际帧长（~62B） | 写量 346→123MB/s（2.8x），均值 1.08→1.53M（+45%），但 std 反升（0.83M），全停 dips 仍在 |
| 物理盘饱和 | Apple Fabric SSD，iostat 恒 15MB/s（页缓存吸收），非带宽瓶颈 |

## 4. 根因

**入站 Archive 录制会话的 I/O 停滞是唯一触发器**：

- `archive-recorder work cycle time exceeded (>100ms)` 稳态下 ~1.5 次/s，
  最大单周期 **2.78s**；`archive-conductor` 最大 2.01s（≈每分钟一次，对齐 purge）；
- 两路录制（入站/成交）停滞**不同步**（同步零进展窗口仅 8/500+），排除进程级饿死，
  指向各自 FileChannel 写路径的 OS 级卡顿；
- 入站录制滞后恒钉在 **33.5MB = 64MB IPC term 的一半**：发布端只能领先录制端
  半个 term（≈0.26s@124MB/s），录制端一卡 >0.26s，整链（ingress→core→egress）
  立即全停；
- 历史对照（08-29"关入站录制 → 4.4M 平线"）证明：成交/深度/快照录制被各自
  64MB 窗口吸收，只有入站录制卡顿会穿透到生产链路。

结论：**锯齿 = 入站录制端 I/O 停滞 × 过小的 IPC 半 term 窗口（反馈环放大）**。

## 5. 已落地修复

1. **入站 journal 变长行**：`[8B 时刻][实际 SBE 帧长]`，去掉 256B 定长零填充；
   journal 写量降 2.8x（这是纯收益，不改语义、不引入恢复缺口）；
2. **可观测性修复**：`AeronMetricsPublisher` 发布端计数器按 label 匹配
   （此前 type=12 恒 0，入站积压指标静默失效）；
3. 探针/分析工具入仓（上述）。

## 6. 下一步（待决策）

- **入站录制有界解耦（journal bridge）**：ingress 只发 live 流，桥接线程
  消费 live → 内部有界缓冲 → 转发 journal 流；录制停滞由缓冲吸收，
  live 路径只在缓冲耗尽时才回压。代价：崩溃时 journal 落后 live 的缺口
  = 缓冲上限（须告警 + 恢复语义改造：锚点改为
  `min(core 位置, 桥已发位置 - 录制滞后)`）；
- 或加大入站流 IPC term（128–256MB）直接放大窗口，代价是多流共享的
  内存增长（本机 16GB 受限）。

---

## 7. 方案 1 落地（2026-09-01 续：journal bridge，已完成 D6–D8 验证）

### 7.1 拓扑

```text
producer → UDP → ingress → feed 流 ──→ journal bridge（ingress 进程内）
                                           ├─→ live 流（core 订阅；仅受 core 流控）
                                           └─→ journal 流（Archive 录制；录制卡顿由
                                                512MB 环形缓冲吸收，feed 端只在环满时回压）
```

桥 = 环形缓冲（默认 256MB，`ocx.journal-bridge.ring-size`）+ 双消费游标
（live/journal 各自独立读指针，行保留到两侧都消费完）+ 稀疏索引耐久定位。

### 7.2 快照锚点（语义改造）

- 桥每 50ms 经 Archive 查询 journal 录制位置，沿稀疏索引（每 1024 行一条
  ringOffset/journalEnd）定位**最新已耐久行**，把 `(durableLive, durableJournal)`
  写入媒体驱动计数器；
- `SnapshotCoordinator` 仅在 `durableLive ≥ core 消费位置` 时冻结，锚点 =
  durableJournal（journal recording 上的字节位置）；
- 消除了原 `min(image, recorder)` 在录制滞后时产生的**静默不一致快照**
  （prefill 状态超前于锚点 → 恢复重复应用）。

### 7.3 journal 保留

- 快照锚点 purge 照旧（删锚点前段）；
- 新增 `ocx.journal.retain-bytes`（默认 32GB）保留兜底：快照被门控跳过时
  每 60s 按字节上限回收，防录制无界增长撑满磁盘；
- **约束：retain-bytes 必须 ≥ 快照间隔 × journal 写率 + 余量**（60s × 470MB/s
  ≈ 28GB），否则保留段可能越过最后有效快照的锚点，恢复无法回放（D8 用 8GB
  配置验证保留时发现该约束）。

### 7.4 关键 bug 修复（D6/D7 实测定位）

1. **Aeron poll 语义坑**：`Subscription.poll` 无论 handler 做什么都会推进
   image 位置——环满时 handler 提前 return 即**静默丢行**（D6：feed 消费
   100GB、live 冻 665MB、fill=0 全对上）。修复：按环空余/最大行预算 poll，
   环满不 poll（回压而非丢弃）；
2. 行跨环边界写 + 消费侧 scratch 拷贝，消除 head 贴环尾死锁；
3. 耐久游标从环偏移 0 起步在环回绕后读到新数据、永不前进（D7：快照全被
   门控跳过、journal 无界 132GB）→ 改稀疏索引定位。

### 7.5 验证结果（D8，300s，5.5M 目标，512MB 环）

| 指标 | D1 基线（无桥） | D8（桥） |
| --- | --- | --- |
| 实际 TPS | 1.43M（稳态均值 1.08M） | **3.09M（稳态均值 2.94M，2.16x）** |
| 稳态 p50 / p90 | 1.05M / 1.69M | **3.30M / 3.36M（顶部平线）** |
| journal 滞后 | 恒 30MB（半 term 上限） | 0.4–4.6MB |
| 丢失 | — | 零（feed/live/journal 三流 139GB 同步） |
| journal 保留 | 无界 | 150GB 总写、保留 22.5GB |

### 7.6 残余问题（下一步）

- **成交录制端仍在 core 关键路径**：trade recording 122MB/s 的录制停滞
  经 egress 队列回压 core（D8 残余 ~4s 周期 dips、std 0.74M 的来源）；
  下一步用同款桥把出站（trade/depth）录制也解耦；
- 恢复/对账在桥模式下的端到端验证（锚点语义改造后需重跑 RecoveryMain +
  TradeRecordReconciliationMain，retain 配置需满足 §7.3 约束）；
- 备机（standby）暂走兼容模式（`ocx.journal-bridge.enabled=false`），
  桥模式接入 HA 拓扑待做。

---

## 8. 方案 1 第二刀：出站（trade/depth）录制解耦（2026-09-02）

### 8.1 实现

成交/深度没有快照锚点语义，不需要 feed 流/双流——在 egress 进程内做
**编码 → `OutboundJournalRing`（SPSC 字节环）→ 独立 drain 线程 → journal 流**：

- 行布局 `[4B len][msg]`，行可跨环边界；环满发送线程自旋（有界回压）；
  无订阅者（零落盘形态）丢弃 + 一次性告警；
- trade 环默认 512MB（`ocx.outbound.trade.ring-size`）、depth 4MB；
- 成交批最大 512×~200B ≈ 100KB——环 maxMsg 由构造参数传入（缺省 8192
  会抛异常丢批，D9 实测 trade 仅录 262MB/300s）。

### 8.2 效果（D10-D15 中段实测）

- trade 以 114MB/s 正常录制（17.4GB/152s），零丢失；
- TPS 3.05M（累计），与 D8 持平；
- 单元测试：20 万条变长消息环回绕转发零丢失 + 回压恢复。

### 8.3 新发现的确定性故障（未修复，下轮）

D10-D15 全形态 300s 均在 ~281s 处**所有 Archive 录制会话同时终止**：

- journal 132.7GB / trade 32.1GB 处 stop（发布端全活、无业务错误）；
- `Archive Errors=3`、`Publications Revoked=2`、rec-pos 计数器消失；
- recorder 线程事后健康空闲（会话被关而非线程崩溃）；
- 已排除：磁盘满（保留生效时 91GB 空闲）、保留 purge（禁用仍复现）、
  untethered 超时调大到 60s 仍复现（吊销发生在会话终止后）；
- 指向：Archive 录制订阅 untethered 语义 + 录制端 >60s I/O 停滞长尾
  （本机环境级）→ 吊销发布端 → 会话终止 → 引擎停摆；出站环非根因
  （D8 同配置无环时未触发）。

**下轮候选**：录制端停滞监控 + 会话自愈（检测 rec-pos 冻结超阈值 →
  告警/重启 Archive）；或改录制订阅为 tethered + 桥环吸收（语义改造）。

---

## 9. 根因确认与修复（2026-09-02 续）

### 9.1 根因（D16 拿到的真实错误）

给 `OcxArchiveMain` 加 stderr 错误输出后，D16 复现 ~281s 停滞，错误为：

```text
[OcxArchive] ERROR: ArchiveException - java.io.IOException - No space left on device
```

**根因链**：journal 保留不可靠 → journal 无界增长 → 磁盘瞬时打满 →
录制段写失败 → 全部录制会话终止（Archive Errors / Publications Revoked）→
引擎停摆。保留不可靠的两个来源：

1. 保留 purge 原在快照线程内：快照被慢速录制拖住时（240MB 快照可拖 200s+）
   purge 完全不跑；
2. 保留线程与快照线程**共用同一个 AeronArchive client**（线程不安全），
   控制请求交错 → purge 静默失败。

### 9.2 修复

- `purgeJournalRetention` 移入独立 `snapshot-retention` 线程 + **独立
  AeronArchive client**；
- 保留窗收紧：默认 16GB / 15s（`ocx.journal.retain-bytes` /
  `ocx.journal.retain-tick-ms`），瞬时磁盘峰值从 ~140GB 降到 ~50GB；
- `OcxArchiveMain` stderr 错误输出（错误计数器消息不可读，此前全靠猜）。

### 9.3 D17 验证（300s 完整通过）

| 指标 | D1 基线 | D8（入站桥） | D17（入站+出站桥+保留修复） |
| --- | --- | --- | --- |
| 实际 TPS | 1.43M | 3.09M | **3.14M** |
| 稳态 mean | 1.08M | 2.94M | **3.09M（2.87x）** |
| p50 / p90 | 1.05M / 1.69M | 3.30M / 3.36M | **3.25M / 3.34M** |
| p10 | 0.55M | 1.75M | **2.43M** |
| std | 0.46M | 0.74M | **0.47M** |
| Archive 错误 | — | — | **0** |

残余 ~4s 周期 dips（p10 2.43M vs p90 3.34M）来源待定：快照批末冻结在
撮合线程（60s 一次 ~100-300ms）、簿容量动态或录制停滞穿透环——下轮可
继续压（快照冻结移出撮合线程等）。

### 9.4 遗留

- ~~桥模式恢复/对账端到端验证（锚点语义改造后）~~ ✅（§10）；
- ~~备机（standby）接入桥模式~~ ✅（§11，2026-09-02）；
- 残余 dips 归因（§9.3）；
- Archive 重启自愈（本次根因已修复，重启自愈降级为可选兜底）。

---

## 10. 桥模式恢复/对账端到端验证（2026-09-02 续）

### 10.1 恢复验证 ✅

- `WalArchiveRecoveryTest` 5/5（修复 3 个存量失败：orderId 重构后槽位由
  freelist 分配，测试仍按"槽位==orderId"断言 → 改按 orderId 扫槽）；
- D18/D19 桥模式全形态 + `--wal-recover-check`：
  `WAL_RECOVERY_DONE`（records 1296 万、openOrders 105.6 万、triggerOrders 40 万）
  ——从 journal recording + 快照锚点完整重建簿状态 ✅。

### 10.2 锚点精确化（D18 对账暴露，已修复）

D18 对账 RECON_FAIL：U5=10674 / U1=2193，全部聚集在锚点边界。根因：
桥模式锚点此前取 **durableJournalPos（最新已耐久行）**，可能超前快照冻结行
（maxImage）→ 恢复从锚点回放跳过冻结行与锚点之间的输入 → 边界成交引用
未知订单。

修复：桥新增 **`journalPosAt(livePos)`**（稀疏索引定位 + 环内逐行前进扫描，
精确到行），core 快照写 META 前经查询/应答控制流（510000/511000 段）取
冻结行（maxImage）对应的精确 journal 位置作锚点。

D19（120s + 恢复 + 对账）：**U5 10674→126、U1 2193→32（-99%）**。

### 10.3 残余（0.002%，下轮收尾）

残余 126 U5 / 32 U1 全在锚点后 1-6ms：

- `U1 首笔 != price×amount`（fromSnapshot=false 的订单首笔带累计额）——
  预填累计额基线 vs 锚点边界首笔成交的基准未完全对齐；
- `U5 inPrefilled=true 但 tracker 空`——预填 sumAmount 基线使对账的
  "订单完成即移除"判定早于引擎实际完成，边界后合法成交被误报幽灵单。

方向：对账锚点边界用"首笔只记基线"的窗口从单帧扩展为锚点后首批
（或按时间窗），并核对预填 executedQty 基线与成交 recording 的累计对齐。

### 10.4 残余归零（2026-09-02 续，已修复）

迭代 D19→D27 把边界违例从 U5=10674/U1=2193 降到 **0**：

| 修复 | 机制 |
| --- | --- |
| 组收齐完整性 | 同 ts 组入站收齐的 250 空转 CAP（≈500ms）在 shared 模式单
  replayer 被 12GB 成交回放占用时提前 break → fills 先于 creates 消费 →
  误报 U5。CAP 提到 2500（≈5s）+ 残缺组放回队首重试 |
| 边界 ts 成交基线 | ts == 增量首帧 ts 的成交不再丢弃（承载订单累计基线），
  只记基线不校验 |
| 同毫秒创建规则 | 订单 createTime == 成交 ts（含 create 已见但条目未重建的
  竞态）→ 只记基线不校验（双回放同 ms 顺序不可辨） |

**验证**：

- D27（90s，1 快照边界）：48M 笔成交 **RECON_PASS，U1/U2/U4/U5 全零**；
- D28（150s，2 快照边界）：48M 笔 U1/U2/U4=0，U5=1（0.000002%，第二个
  快照锚点后 10ms 的预填 tracker 书签边角，非引擎缺陷，FAIL 可见不静默）；
- `TradeRecordReconciliationTest` 5/5 无回归。

## 11. 备机接入桥模式（2026-09-02，已完成）

> 编排/压测脚本自 2026-09-02 统一收口至 `ocx-match-bench/scripts/`
> （自 ocx-match-core 迁入）；本节 `standby*.sh` 命令均从
> `cd ocx-match-bench/scripts` 后执行。

### 11.1 改动

- `StreamIds.standbyJournalEffective`：bridge 模式录备机 journal
  （inboundJournal），兼容模式录 standbyInbound；
- `StandbyArchiveMain`：复制主机 **effective journal recording**
  （bridge 模式 = inboundJournal），常驻录制备机 effective journal；
- `ReplicationIngressBridge`：bridge 模式发布到备机 **feed 流**，经备机
  journal bridge 转发 live（standbyInbound）+ journal；修复其 streamId 仍按
  live 流找 recording 导致 bridge 永不建立 replay 的 bug；
- 脚本：standby 系列开 bridge + untethered 60s；修非 Raft 协调者 53000
  与 bridge 冲突（协调者 listen 挪 53010、引擎心跳指向）；备机 egress 堆
  512MB→1g（装下默认 512MB 出站环）。

### 11.2 验证

- **主备一致**（30s、100K TPS、bridge 模式）：host=831633 trades ==
  standby=831633，簿 1993==1993，0 rejects；
- **故障接管**（--failover）：主备 473523==473523 → kill 主机 → 协调者
  PROMOTE → bridge 停止 → producer2 打备机 ingress → 接管后成交
  473523→1,015,137 持续增长、0 rejects、无回退。

备机全链路（复制 → replay → feed → 备机 bridge → live/journal）与主机
同构，快照锚点/保留/对账均走备机自身 driver 的计数器与 recording。

### 11.3 一主两备 + Raft 仲裁桥模式验证（2026-09-02，standby2-test.sh）

主机 + 备机1/备机2 + Raft 3 节点协调组，150K TPS × 15s：

- **三实例一致**：host=sb1=sb2=637853 trades、簿 56245==56245==56245、
  0 rejects；
- **仲裁组自愈**：kill 仲裁 leader（arb1）→ 新 leader 当选、主机续租未中断；
- **双备选一接管**：kill 主机 → 仲裁组提升 standby=2（EPOCH=2）→
  接管后 trades 637853→1,469,234 持续增长、无回退；落选备机保持 standby
  （无新输入不增长属正常），两台备机 SELF_FENCED=0（未误自杀）。

桥模式在一主两备 + Raft 仲裁路径下与单备非 Raft 同构通过。

### 11.4 故障演练矩阵（2026-09-02，standby-test.sh --drill-*）

| 演练 | 步骤 | 结果 |
| --- | --- | --- |
| 仲裁双挂 | kill arb2+arb3（组失多数派） | 主机 SELF_FENCED（成交停增）、
  PROMOTE_SENT=0、备机未接管（无双主）；重启被杀节点后组恢复重新选举 ✅ |
| 网络分区 | SIGSTOP 主机 core（与仲裁断连） | 协调者提升备机（EPOCH=2）；
  SIGCONT 后主机自熔断（不再输出，无双主）✅ |

演练发现并修复生产 bug：**仲裁节点 kill -9 后重启失败**（Ratis
"Failed to FORMAT: existing directories found"）——仲裁存储改为启动前清空
（epoch/租约全在 Raft 日志，重启节点从 leader 重同步，存储易失）。

## 12. 残余 dips 归因（2026-09-02：机器热/CPU 包络，非引擎缺陷）

### 12.1 证据

- **D8/D17（300s）**：30-260s 稳态 mean 3.31M、std 0.09M（±3%）——已是干净平线；
  "std 0.47M / 4s 自相关"是统计窗（最后 180 间隔）被尾部 ~40s 退化污染的假象；
- **D29（600s，5.5M 目标）**：退化始于 ~240s 并持续（30-240s 稳 3.3M →
  240s 后 1.6-2.4M）；**D30（300s）onset ~210s**——onset 跨 run 漂移；
- **D31（600s，2.5M 目标）**：前 300s 完美平线 2.50M（min=2.50M！），
  ~330s 后退化到 ~1.8M——**负载越低 onset 越晚**；
- 退化期间 producer retries 保持 ~750/s 不变（非更强背压）、簿大小稳定
  （~1.4M 上限，远离 5M cap）、core 零 GC（gc_young=0~1）。

### 12.2 结论

**late-run 退化 = 机器热/CPU 包络**（4P+6E MacBook 持续满载数分钟后节流；
与 08-30"对照组被热节流污染（越靠后越慢）"一致），非引擎缺陷。引擎的
**稳态全形态能力（含入站/出站双解耦 + 全录制 + 快照）在 2.5M 与 3.3M
下均为干净平线**（±3%，直至热墙）。

生产口径：真实服务器（独立核/NVMe/主动散热）下稳态平线应能维持；
本机压测口径须剔除尾部热退化窗（稳态窗取 30s ~ onset-30s）。

## 13. D32：30 分钟长跑 + 恢复 + 对账（2026-09-02，全形态）

### 13.1 场景

桥模式全形态（journal bridge 512MB 环 / 快照 60s / journal retain 32GB），
1.5M 目标 × 1805s（5s 预灌 + 30min 撮合），cap=5M、core 6g、单 producer
确定性用户（--fund-users 100）；收尾自动跑 ArchiveProbe + RecoveryMain +
TradeRecordReconciliationMain（快照锚点增量）。

产物 `/tmp/perf-d32-1800s`（关键文件归档 `/tmp/perf-d32-data`，run 日志
`/tmp/perf-d32-1800s-run.log`）。

### 13.2 结果

**长跑（30min 连续注入 27.08 亿单）**

| 项 | 值 |
| --- | --- |
| 实际 TPS | 1,500,001（目标 1.5M，精确平线） |
| 总发送 | 2,707,503,000 条（发送失败 0） |
| 背压退避 | 25,820 次（≈14/s，占发送 0.001%） |
| core GC | young=3 / full=0（30min） |
| 引擎/录制异常 | 0（archive/core/ingress/egress/driver 日志全净） |

TPS 轨迹：producer 2s 采样 avg 从 1.481M（t=264s）缓升到 1.497M+（t≥1500s），
**全程无热墙 onset**（对照 D31：2.5M 目标 onset ~330s）——1.5M 负载下
30min 持续是干净的稳态线，late-run 退化确属高负载热墙而非引擎缺陷。

**录制与磁盘水位（30min 全形态）**

- 成交流（227916）：9.63 亿 fills（0.355 fills/order），全量 91.7GB 落盘；
- journal 流（507916）：累计位置 433.2GB，保留末段 ~13.8GB（≈60s 快照锚点
  窗），purge 正常——"journal 无界增长"未复现；
- 快照流：保留末段 ~0.9GB（最近 2-3 张）；深度流 33.7MB；
- archive 峰值 ~99GB，系统可用 254GB→155GB，全程水位可控。

**恢复（RecoveryMain）**：WAL_RECOVERY_DONE records=85.66M，从最后快照 +
尾段 journal 重建 openOrders=500,371 / triggerOrders=1,048,576，无报错。

**对账（TradeRecordReconciliationMain，快照锚点增量 ~57s）**：
creates=45.67M / cancels=39.99M / fills=30.37M；
U1 金额 / U2 订单守恒 / U4 自成交 / U5 引用批撤 **全 0，RECON_PASS**。

### 13.3 结论

30 分钟长跑全形态（录制/快照/保留/恢复/对账全链路）验证通过：1.5M TPS
精确平线、零错误零停摆；journal 保留与快照回收把磁盘水位压在 ~100GB 级；
崩溃恢复与锚点增量对账在长跑尾部窗口均干净通过。


