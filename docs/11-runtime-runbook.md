# 撮合引擎跨机启动运行手册

> **本章解决**：如何在两台机器上启动、验证、排障。
> **前置依赖**：08（部署）、09（监控）。
> **读者前置**：命令行 / SSH / Docker。

> 迁移自 open-cex-docs `references/11-runtime-runbook.md`（2026-09-05 迁入 open-cex-match/docs）。

> 适用范围：`open-cex-match/ocx-match-core`、`ocx-match-ingress`、`ocx-match-egress`、`ocx-metrics-exporter`
> （同仓 `ocx-codec` 提供 SBE 契约；撮合域已从 `open-cex-business` 迁出）
> 最后更新：2026-08-28
> 关联文档：`match-engine-*.md`（本目录设计文档）、`12-stress-test-report.md`

## 0. 先记住三件事

1. 撮合引擎已经 headless：**不监听任何入站 HTTP，也不主动 push**。不要再 curl 它的 `/actuator/health`，判活只能看 sidecar 暴露的 `match_engine_up`。
2. 每个实例必须独占自己的 **symbol**，并共享 **媒体驱动目录 + Archive 记录目录**
   （撮合链路已完全脱离 Kafka；2026-08-30 起 metrics 也走 Aeron 流，不再有独立
   metrics 队列目录，见 §2 五进程形态）。
3. 105 是 2 物理核，适合做兼容性/低负载验证；120 是 10 核 Apple M4，是当前压测和主要验证环境。
4. **Grafana 有数据的前提是 sidecar 在跑**：引擎指标经 metrics(symbol) Aeron 流直发
   sidecar，Prometheus 抓 sidecar 暴露的 `:9201/metrics`。压测/观察前必须确认
   `screen -ls` 里有 `metrics-exporter` 且 `curl -s localhost:9201/metrics | grep match_engine_up`
   返回 1，否则 Grafana 全程无数据（2026-08-24 事故：多轮压测 sidecar 未启动）。
   **启动顺序不再敏感**（2026-08-30）：sidecar 常驻，媒体驱动/引擎晚起时每秒重试建连。

> 快照（2026-08-31 起）：**Aeron Archive 常驻 recording + BEGIN 分段**——快照流由
> `OcxArchiveMain` 启动时常驻录制，每张快照 = `BEGIN(seq)+META+ORDER+STOP+END`
> 分段，恢复端逐段校验取最新有效段；恢复 = 回放最新有效快照段预填 + 从 META
> 锚点增量回放入站（见设计文档 06-crash-recovery.md §1.4 / 10-performance.md §1.9 ）。不再每张快照 start/stop recording
> （运行中临时 startRecording 会踩 Aeron available-image 回调缺陷）。

> 2026-08-23：105 上的 `ocx-market-maker` 会持续向 `spot_in.BTCUSDT` / `spot_in.ETHUSDT` 写单。
> 任何“干净压测”开始前必须 `docker stop ocx-market-maker`，压测期间保持停止；否则引擎启动后
> `match_orders_total` 不会从 0 开始，数据会混入背景做市订单。

## 1. 两台机器的差异

| 维度 | 105（开发/低负载） | 120（压测/主验证） |
| --- | --- | --- |
| CPU | Intel i5-5200U，2 物理核 / 4 逻辑核 | Apple M4，10 核 |
| 内存 | 12 GB | 16 GB |
| Archive 记录目录 | 远程/本机均为引擎机磁盘（`-Docx.archive.dir=`，默认 `<aeronDir>/archive`） | 120 压测：`$OUT/archive` |
| 引擎运行方式 | Docker，`--network host` + `--cpuset-cpus` | 裸 Java + `screen`（不建议 `nohup`，见 §7） |
| 绑核 | `--cpuset-cpus` + `-Daffinity.reserved` | macOS 无 `sched_setaffinity`，Affinity 自动降级 |
| wait strategy | `sleeping`（省 CPU） | `yielding`（低延迟；空闲时核仍近 100%） |
| metrics sidecar 端口 | `9101`（默认） | `9201`（避免与 105 默认端口冲突） |
| Prometheus 抓取 | 105 本机 Prometheus | 105 Prometheus 抓 `192.168.0.120:9201` |

> 105 上的脚本 `deploy-obs-105.sh` 是机器专属维护脚本，不在仓库内；本手册把其中的参数和原因固化到仓库文档，避免再靠口头/临时脚本传述。

## 2. 构建产物

```bash
cd open-cex-match
mvn -U clean package -DskipTests \
  -pl ocx-match-core,ocx-match-ingress,ocx-match-egress,ocx-metrics-exporter -am
```

产物：

- 引擎 shade jar（含 core/ingress/egress 全部主类，可经 `-cp` 指定任意装配根）：
  `ocx-match-ingress/target/ocx-match-ingress-1.0.jar`
- sidecar：`ocx-metrics-exporter/target/ocx-metrics-exporter-1.0.jar`

跨机部署时两个 jar 都要同步；引擎和 sidecar 版本必须来自同一次构建，避免队列字段契约不一致。

> **2026-08-28 五进程架构**：生产形态为四个独立业务进程 + 独立媒体驱动——
> `com.ocx.ingress.MatchIngressBootstrap`（Aeron UDP 收单/解码/发布入站环）、
> `com.ocx.MatchCoreBootstrap`（纵表撮合，零 Kafka/零 SBE/零磁盘）、
> `com.ocx.egress.MatchEgressBootstrap`（WAL/Aeron Archive 出站/metrics）、
> `com.ocx.egress.OcxArchiveMain`（Aeron Archive 持久化日志，撮合链路最终出口）、
> `com.ocx.match.aeron.OcxMediaDriverMain`（共享 Aeron IPC 媒体驱动，`-Docx.aeron.dir=` 指定目录）。
> 进程间传输为 Aeron IPC（共享内存），开发态 `ocx-match-ingress-1.0.jar` 用嵌入式驱动单 JVM 验证同一条路径。
>
> **2026-08-28 起压测/快测默认五进程编排**（`ocx-match-core/stress-test.sh --big`）：
> 媒体驱动 → Archive → egress → core → ingress 五 JVM 依次启动（逐进程等控制流
> READY，2026-08-30 起替代 .out 文件 grep），共享
> `-Docx.aeron.dir=$OUT/aeron`，每进程独立 JFR/GC 日志，停机按倒序 TERM；
> `--no-affinity` 保留（只传 core）。单 JVM 装配根（`com.ocx.MatchEngineBootstrap`）已于
> **2026-08-30 删除**——生产/压测统一走五进程形态（media driver + Archive + egress +
> core + ingress），各进程从 ingress fat jar 经 `-cp` 指定主类启动。
> 为开发入口，不再用于压测——单进程下多线程合计易被误判为引擎能力。
> Aeron IPC MTU 默认 1408B：出站订阅已包 `FragmentAssembler`（超 MTU 批量帧分片重组），
> WAL 批量帧限制 ≤1400B 保持单帧零拷贝路径。

## 3. 引擎启动参数模板

### 3.1 所有实例都需要的 JVM 参数

```text
-Xms512m -Xmx2g
-XX:+UseG1GC
-XX:MaxGCPauseMillis=20
-XX:+AlwaysPreTouch
--add-modules=jdk.management
--add-opens java.base/java.lang.reflect=ALL-UNNAMED
--add-opens java.base/java.nio=ALL-UNNAMED
--add-opens java.base/jdk.internal.misc=ALL-UNNAMED
--add-exports java.base/sun.nio.ch=ALL-UNNAMED
-Xlog:gc*:file=/app/logs/gc.log:time,uptime,level,tags:filecount=10,filesize=20m
-Xlog:safepoint=debug:file=/app/logs/safepoint.log:time,uptime,level,tags:filecount=10,filesize=20m
-XX:StartFlightRecording=disk=false,maxsize=256m,settings=default,name=match-engine
-Daes.allow-plaintext=true
-Dmatch.self-trade-prevention=false
```

说明：

- **固定堆 + AlwaysPreTouch（2026-08-27）**：撮合核心（纵表容量扩张）用**固定堆**
  （压测/高容量基准 `-Xms4g -Xmx4g`（2026-08-28 起默认，原 6g；纵表 5M 簿峰值仅 ~1GB），
  常规小堆 `-Xms2g -Xmx2g`）并追加
  `-XX:+AlwaysPreTouch`——启动时把全部堆物理页 pre-touch（mmap 填零），
  杜绝撮合过程中动态物理页分配（Page Allocation）带来的微秒级抖动（LMAX 机械同理心）。
- `jdk.internal.misc` 是 Chronicle/Agrona 在当前实现下额外需要的，不能只保留老的 `java.lang.reflect`/`java.nio`/`sun.nio.ch`。
- `--add-modules=jdk.management` 显式暴露 `com.sun.management`，供引擎 metrics 导出线程读取最近一次 GC pause。
- GC 和 safepoint 使用 JDK 统一日志，落盘到 `/app/logs`（120 裸机模板会改成 `/tmp` 下对应文件）。日志轮转由 `filecount/filesize` 控制，避免长期运行写爆磁盘。
- JFR 采用内存 ring buffer，不持续落盘；需要排查时用 `jcmd <pid> JFR.dump name=match-engine filename=/tmp/engine.jfr` 再导出。
- `-Daes.allow-plaintext=true` 和 `-Dmatch.self-trade-prevention=false` 是开发/压测配置，生产环境必须替换为真实密钥和风控决策。
- 堆大小 `512m/2g`：`512m` 在 300 万订单积压下快照任务会 OOM（105 早期结论）；2026-08-24
  恢复 748 万条 WAL（540 万订单压测）实测 `1g` 堆 OOM、`2g` 稳定——**恢复大数据量 WAL 时
  堆必须 ≥2g**。常规运行按需增长，`-Xms512m` 保留以降低闲置占用。

### 3.2 每个实例必须显式给出的参数

| 参数 | 作用 | 示例 |
| --- | --- | --- |
| `-Dspring.profiles.active` | `spot` 或 `futures`，决定 profile 配置 | `-Dspring.profiles.active=futures` |
| `-Dspring.match.symbols.{SYM}.coinScale` | 白名单币对 | `-Dspring.match.symbols.BTCUSDT.coinScale=8` |
| `-Dspring.match.symbols.{SYM}.baseCoinScale` | 白名单币对 | `-Dspring.match.symbols.BTCUSDT.baseCoinScale=8` |
| `-Docx.ingress.aeron.udp.endpoint` | Aeron UDP 收单端点（2026-08-28 起收单不走 Kafka） | `-Docx.ingress.aeron.udp.endpoint=0.0.0.0:11000`（同机多实例须换端口） |
| `-Docx.aeron.dir` | 共享媒体驱动目录（媒体驱动/ingress/core/egress/Archive 必须一致） | `-Docx.aeron.dir=/tmp/ocx-aeron` |
| `-Docx.archive.dir` | Archive 记录目录（仅 Archive 进程使用，默认 `<aeronDir>/archive`） | `-Docx.archive.dir=/tmp/ocx-aeron/archive` |
| `-Dmatch.wal.data-dir` | 仅单 JVM 开发形态的内嵌 Archive 目录；生产快照/入站 recording 均在 `-Docx.archive.dir=`（2026-08-30 起无独立快照目录） | 120：`/Users/bot01/match-wal-futures-btcusdt`；105 容器内：`/data/match/wal` |
| `-Docx.snapshot.interval-ms` | 快照周期 ms（默认 10000；0=关闭）。撮合线程批末只冻结（实测 ~10-20ms），编码/发布/回收在 snapshot-writer 线程 | 压测对照用 `0`；生产默认 `10000` |
| `metrics.aeron.dir`（sidecar） | sidecar 订阅 metrics 流所需的共享媒体驱动目录（必须与引擎 `-Docx.aeron.dir=` 一致） | 120：`/tmp/ocx-aeron`；105 容器内：与引擎 aeron dir 同卷 |
| `metrics.symbols`（sidecar） | sidecar 订阅的 symbol 列表（逗号分隔；2026-08-30 起按流订阅，替代按目录发现） | `BTCUSDT` |
| `metrics.profile`（sidecar） | Prometheus 标签 profile（2026-08-30 起由配置提供，替代从队列目录名解析） | `spot` |

可选：

| 参数 | 作用 | 使用场景 |
| --- | --- | --- |
| `-Docx.match.idle-strategy` | 撮合线程 idle：`backoff`（默认，激进自旋+偶发让核）/`busy-spin`（独占核/Linux 绑核低延迟）/`yielding`/`sleeping` | 共享核（macOS 4 P-core）默认 `backoff`；独占核场景 `busy-spin`（LMAX 标准） |
| `-Daffinity.reserved=<hex>` | 把 OpenHFT 选核池收敛到容器 cpuset | 仅 105 Docker + cpuset 场景 |
| `-Dmatch.wal.enabled` | 显式开关 WAL | 默认 `true`；本仓库 spot/futures profile 默认值不同，建议显式确认 |
| `-Dmatch.wal.archive-control-channel` | Aeron Archive 控制请求通道（恢复回放走同一控制面） | 默认 `aeron:udp?endpoint=localhost:8010`；跨机/跨容器改为 Archive 实际地址 |
| `-Dmatch.wal.archive-control-response-channel` | Aeron Archive 控制响应通道 | 默认 `aeron:udp?endpoint=localhost:0`（自动分配端口） |
| `-Docx.archive.file.sync.level` | Archive 文件同步级别：`0`=NO_SYNC（默认）/`1`=SYNC/`2`=FSYNC | 压测/开发默认 `0`；生产按 RPO 需要开 `1`/`2`（FSYNC 掉吞吐，4M 压测未开） |
| `-Dspring.match.max-open-orders` | 订单簿挂单上限（内存硬闸） | 默认 `6000000`（配合 4g 堆，高峰 500 万在簿 + 20% 余量，2026-08-25 标定）。实测标定（4G 堆）：5M 在簿 heap 峰值 1.67GB，0 Full GC（压测报告 §4.20.5/§13.8.4）；厚簿场景按堆预算上调，见设计文档 §13.8 |

> 注意：当前代码读的是 `-Dmatch.wal.data-dir`（`RecoveryConfig` 前缀 `match.wal`），不是旧文档里的 `MATCH_WAL_DATA_DIR` 环境变量。

**WAL 容量预算（2026-08-28 Archive 口径）**：4M TPS × 60s 压测实测 Archive recording
（含 WAL 流 + 成交/深度/DLT 流）≈ 5.2GB，即约 21MB/100 万事件；500K TPS/symbol
稳态约 39GB/小时。磁盘按 Archive 目录（`-Docx.archive.dir=`）统一规划：12h ≈ 470GB、
24h ≈ 940GB；清理走"停 Archive → 目录整体移入回收站重建"（§7 注意点 8），
不要在 Archive 运行中删 recording 文件。

**长跑实测口径（2026-08-30，5.5M × 600s，设计文档 10-performance.md §1.9 ）**：完整形态持续
吞吐均值 ~1.6M TPS（100s ≈3.7M → 600s ≈1.6M 衰减，归因 = 入站 recording 落盘 +
页缓存累积；关录制对照 4.47M 平线）。入站 recording 全程 ~310GB（≈0.51GB/s），
快照后段回收把保留水位压在 ~754MB；**入站录制为主盘最重负载**，持续高吞吐部署
建议把 `-Docx.archive.dir=` 放到独立介质，或按业务持久性需求降频录制。

## 4. 120 裸机启动命令（当前压测环境）

120 上 4 个引擎以 `screen` 常驻；不要直接 `nohup ... &`，`exec_command` 环境会把后台进程回收。

**日志策略（2026-08-24 起）**：应用日志走 logback 滚动文件（`ocx-logging` 模块），
`LOG_PATH` 指向 `$WAL_ROOT/match-engine-logs`，`app.log` 50MB×7/500MB、`error.log`
20MB×14/200MB 自动滚动；stdout 重定向到 `/dev/null`（**不要再重定向到无滚动的
`/tmp/engine-*.log`**——30 分钟 cap 拒绝压测实测无滚动单文件膨胀到 10.7GB）。
排障 tail：`tail -f $WAL_ROOT/match-engine-logs/app.log`；GC/JFR 仍在 `/tmp`（GC 日志自带
`filecount/filesize` 滚动）。判断引擎是否正常启动看 `app.log` 里的
`MATCH_CORE_READY` / `MATCH_EGRESS_READY` / `WAL 恢复完成`。

**引擎日志用纯文本（非 JSON）**：共享 `ocx-logging` 配置的 app.log 是 JSON 行（供日志采集
管道用），撮合引擎没有采集器且拒绝洪峰下 JSON 字符串构建实测占 AsyncAppender ~28% CPU——
引擎启动必须传 `-Dlogging.config=classpath:logback-match-engine.xml` 启用引擎专用纯文本
配置（热路径无 JSON 开销）。其它业务模块不受影响，继续用共享 JSON 配置。

**热路径逐事件日志：压测/生产默认关闭，开发/调试可开**（2026-08-24）：

**堆配置（2026-08-24 起 4G 基线）**：`-Xms1g -Xmx4g`。2G 堆在 400K×300s 实测出现
54 次 Full GC（累计 6.26s、单次最大 300ms），把稳态卡在 ~35 万/s；4G 堆 Full GC 归零，
上限抬到 ~40 万/s（压测报告 §4.16/4.17）。4G 的代价是单次 Young 停顿约 19ms→54ms，
对"宁要短停顿、不要长停摆"的取向更有利。

**年轻代配置（2026-08-25 起 1.5G 基线）**：`-Xmn1536m`。500K TPS × 5M 在簿 × exp
现实订单模型实测（JFR 150s）：相对默认年轻代，晋升老年代从 10.7 万次降到 ~6 千次
（Kafka 消费端短命对象大多死在年轻代，老年代零 Full GC）；相对 `-Xmn2g`，单次暂停
长尾更干净（p99 38ms、max 55ms，无 200ms 尖峰），总 STW 更低。结论：1.5G 是
"长稳（少晋升）+ 延迟稳定（无长暂停）"的折中基线。

- 逐事件日志（cap 拒绝、重复 orderId、tickSize/stepSize 校验、条件单注册/触发、FOK 拒绝）
  已降为 DEBUG 级别——默认 INFO 下不输出；明细由指标
  （`match_orders_rejected_total{reason=...}`、`match_order_book_size` 等）+ Kafka
  `{profile}_order_reject` topic 覆盖，日志只留异常级诊断（发送失败/解码失败/校验失败/
  撮合耗时偏高仍为 WARN/ERROR）；
**引擎零业务日志（2026-08 起）**：不输出任何业务日志（`slf4j-nop`），
可观测性由 sidecar 指标 + 关键事件通道承载；stderr 仅兜底就绪标记与 JVM 未捕获异常。

```bash
JAR=/Users/bot01/open-cex-repos/open-cex-match/ocx-match-ingress/target/ocx-match-ingress-1.0.jar
METRICS=/Users/bot01/match-engine-metrics
WAL_ROOT=/Users/bot01

COMMON_JVM="-Xms1g -Xmx4g -Xmn1536m -XX:+UseG1GC -XX:MaxGCPauseMillis=20 \
--add-modules=jdk.management \
--add-opens java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens java.base/java.nio=ALL-UNNAMED \
--add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
--add-opens java.base/sun.nio.ch=ALL-UNNAMED \
--add-exports java.base/sun.nio.ch=ALL-UNNAMED \
-Dlogging.config=classpath:logback-match-engine.xml"

run_engine() {
  local name="$1" profile="$2" symbol="$3"
  local lower
  lower=$(echo "$symbol" | tr '[:upper:]' '[:lower:]')
  screen -dmS "$name" bash -c \
    "java $COMMON_JVM \
      -Xlog:gc:file=/tmp/engine-gc-$profile-$symbol.log:time,uptime,level,tags:filecount=5,filesize=50m \
      -XX:StartFlightRecording=filename=/tmp/engine-$profile-$symbol.jfr,settings=profile,disk=true,maxsize=3g \
      -Dspring.profiles.active=$profile \
      -Dspring.match.symbols.$symbol.coinScale=8 \
      -Dspring.match.symbols.$symbol.baseCoinScale=8 \
      -Dspring.match.max-open-orders=6000000 \
      -Docx.ingress.aeron.udp.endpoint=0.0.0.0:11000 \
      -Dmatch.wal.data-dir=$WAL_ROOT/match-wal-$profile-$lower \
      -jar $JAR \
      > /dev/null 2>&1"
}

run_engine match-spot-btcusdt    spot    BTCUSDT
run_engine match-spot-ethusdt    spot    ETHUSDT
run_engine match-futures-btcusdt futures BTCUSDT
run_engine match-futures-ethusdt futures ETHUSDT
```

sidecar：

```bash
SIDECAR_JAR=/Users/bot01/open-cex-repos/open-cex-match/ocx-metrics-exporter/target/ocx-metrics-exporter-1.0.jar
screen -dmS metrics-exporter bash -c \
  "java \
    --add-opens java.base/java.lang.reflect=ALL-UNNAMED \
    --add-opens java.base/java.lang=ALL-UNNAMED \
    --add-opens java.base/java.util=ALL-UNNAMED \
    --add-opens java.base/java.io=ALL-UNNAMED \
    --add-opens java.base/java.nio=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    --add-opens java.base/java.nio.channels.spi=ALL-UNNAMED \
    --add-exports java.base/sun.nio.ch=ALL-UNNAMED \
    -jar $SIDECAR_JAR \
    --server.port=9201 \
    --metrics.aeron.dir=/tmp/ocx-aeron \
    --metrics.symbols=BTCUSDT \
    --metrics.profile=$profile \
    > /tmp/sidecar-120.log 2>&1"
```

验证：

```bash
screen -ls
curl -s localhost:9201/metrics | grep '^match_engine_up'
# 出站落盘检查（媒体驱动与 Archive 运行时执行）：
java -Docx.aeron.dir=/tmp/ocx-aeron -cp $JAR com.ocx.egress.ArchiveProbe \
  -Docx.probe.symbol=BTCUSDT
```

## 5. 105 Docker 启动命令（低负载/兼容性验证）

105 资源少，关键差异是：

- `--network host`；
- `--cpuset-cpus` 指定逻辑核；
- `-Daffinity.reserved` 收敛 OpenHFT 选核池；
- `-Docx.match.idle-strategy=sleeping`（共享核低资源场景，撮合线程空闲睡眠省 CPU）；
- 出站为 Archive（本机磁盘），无 Kafka；
- WAL/快照 recording 都在 Archive 目录（`-Docx.archive.dir=`）；`-Dmatch.wal.data-dir`
  仅单 JVM 形态内嵌 Archive 用，容器内挂到 `/data/match/wal`（见下）；
  Archive recording 目录（`-Docx.archive.dir=`）也必须挂宿主机卷——WAL 持久化由
  Archive 承担，容器重建后 recording 必须还在；
- metrics 根目录挂到容器 `/metrics`，sidecar 只读挂同一个宿主机目录。

单实例模板：

```bash
APP_JAR=/home/bot01/match-engine/app.jar
SIDECAR_JAR=/home/bot01/match-engine/metrics-exporter.jar
LOG_DIR=/mnt/my_mount_point/ocx-logs/match-engine
WAL_DIR=/home/bot01/data
METRICS_DIR=/mnt/my_mount_point/ocx-logs/match-engine-metrics

name=ocx-match-engine-futures-btcusdt
cpuset=2
profile=futures
symbol=BTCUSDT
sym_lower=btcusdt
reserved=$(printf '%x' $((1 << cpuset)))

docker rm -f "$name" >/dev/null 2>&1 || true
docker run -d --name "$name" \
  --restart always \
  --network host \
  --cpuset-cpus "$cpuset" \
  -v "$WAL_DIR/match-wal-$profile-$sym_lower":/data/match/wal \
  -v "$APP_JAR":/app/app.jar:ro \
  -v "$LOG_DIR/$name":/logs \
  -v "$METRICS_DIR":/metrics \
  eclipse-temurin:21-jre java \
    -Xms512m -Xmx2g -XX:+UseG1GC -XX:MaxGCPauseMillis=20 \
    --add-modules=jdk.management \
    --add-opens java.base/java.lang.reflect=ALL-UNNAMED \
    --add-opens java.base/java.nio=ALL-UNNAMED \
    --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    --add-exports java.base/sun.nio.ch=ALL-UNNAMED \
    -Xlog:gc*:file=/logs/gc.log:time,uptime,level,tags:filecount=10,filesize=20m \
    -Xlog:safepoint=debug:file=/logs/safepoint.log:time,uptime,level,tags:filecount=10,filesize=20m \
    -XX:StartFlightRecording=disk=false,maxsize=256m,settings=default,name=match-engine-${profile}-${symbol} \
    -Daes.allow-plaintext=true \
    -Dmatch.self-trade-prevention=false \
    -Docx.match.idle-strategy=sleeping \
    -Daffinity.reserved="$reserved" \
    -Dspring.profiles.active="$profile" \
    -Dspring.match.symbols."$symbol".coinScale=8 \
    -Dspring.match.symbols."$symbol".baseCoinScale=8 \
    -Docx.ingress.aeron.udp.endpoint=0.0.0.0:11000 \
    -jar /app/app.jar
```

sidecar：

```bash
docker run -d --name ocx-metrics-exporter \
  --restart always --network host \
  -v "$METRICS_DIR":/metrics:ro \
  -v "$SIDECAR_JAR":/app/app.jar \
  eclipse-temurin:21-jre java \
    --add-opens java.base/java.lang.reflect=ALL-UNNAMED \
    --add-opens java.base/java.lang=ALL-UNNAMED \
    --add-opens java.base/java.util=ALL-UNNAMED \
    --add-opens java.base/java.io=ALL-UNNAMED \
    --add-opens java.base/java.nio=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    --add-exports java.base/sun.nio.ch=ALL-UNNAMED \
    -jar /app/app.jar
```

## 6. Prometheus / Grafana 接入

Prometheus 只抓 sidecar，不抓引擎。仓库配置为 `open-cex-ops/prometheus.yml`：

```yaml
- job_name: match-engine
  metrics_path: /metrics
  scrape_timeout: 8s
  static_configs:
    - targets: [192.168.0.120:9201]   # 105 本机抓取时用 localhost:9101
      labels:
        instance: ocx-metrics-exporter
```

验证链路：

```bash
# 120：sidecar 是否出数
curl -s localhost:9201/metrics | grep '^match_engine_up'

# 105：Prometheus 是否能抓到该 target
curl -s 'http://localhost:9090/api/v1/query?query=match_engine_up'
```

Grafana 面板使用 `profile` + `symbol` 双变量。切换抓取目标后，旧 TSDB 里的测试数据没有意义，应先清空再重跑。

## 7. 注意点与常见坑

1. **不要用引擎端口做健康检查**。headless 后 `spring.main.web-application-type=none`，没有 Tomcat。用 `match_engine_up`；sidecar 超过 `metrics.stale-after-seconds`（默认 10s）未读到心跳会置 0。
2. **UDP 收单端点必须按实例隔离**（2026-08-28 起）。同机多实例必须分配不同端口
   （`ocx.ingress.aeron.udp.endpoint`）；流 ID 按 symbol 派生，实例只订阅自己的 symbol 流。
3. ~~`match.consumer-group`~~：Kafka 收单已移除，消费组配置不再存在。
4. **WAL 与快照 recording 均在 Archive 目录（2026-08-30 起无独立快照目录）**。
   入站 journal、成交/深度/DLT、snapshot recording 全部落在 `-Docx.archive.dir=`，
   实例间不再共享写目录；快照流由 Archive 常驻录制（`BEGIN(seq)` 分段，2026-08-31
   方案 A），快照段级回收为 TODO（当前常驻累积）。
5. **指标走 metrics 流（2026-08-30 起，无独立队列目录）**。core/egress 把
   METRICS/EVENT/OUTBOUND_STATS 行发到 `metrics(symbol)` 流，sidecar 订阅消费；
   sidecar 常驻、媒体驱动/引擎晚起时每秒重试建连，启动顺序不再敏感。
6. **105 必须同时给 `--cpuset-cpus` 和 `-Daffinity.reserved`**。OpenHFT Affinity 不感知容器 cpuset，若选中的核不在 cpuset 内会抛 `IllegalStateException`，表现为应用起来了但撮合线程没起。
7. **120 用 `screen`，不要用裸 `nohup`**。在当前执行环境里裸后台进程会被回收；`screen -dmS` 能保持存活。
8. **清理积压时不要 `rm -rf`**。WAL 目录清空用 `find <dir> -mindepth 1 -delete`；
   Archive 旧 recording 在 Archive 进程停止后把 `-Docx.archive.dir=` 目录整体移入
   回收站重建（压测脚本已自动处理），不要在 Archive 运行中删文件。
9. **收单背压语义**：Aeron 收单下没有 Kafka 日志可查积压，`match_consumer_lag` 现表示
   入站 IPC 环积压（发布位置 − 订阅位置）；生产端 offer 负返回退避即背压信号。
10. **sidecar 端口冲突**：默认 `9101`；如果同机已有其他 exporter，或跨机 Prometheus 要区分，`--server.port=9201`。改端口后必须同步 Prometheus 配置。
11. **构建后先检查 jar 是否更新**：跨机部署时最容易出现“代码改了但 jar 没同步”的假阴性。`unzip -l target/*.jar | grep <关键类>` 或比对文件时间。
12. **不要在生产把 `-Dmatch.self-trade-prevention=false` 当成默认**，这是压测方便观察成交的开关。
13. **持久性与恢复语义（2026-08-28 Aeron Archive；2026-08-30 快照同构）**：
    入站帧与快照都由 Archive recording 承载；恢复先回放 snapshot recording
    （META+END+计数校验，无 END 回退上一份）预填簿，再从 META 锚点
    （`inboundRecordingId`/`inboundPosition`）增量回放入站。崩溃丢最近未 fsync 窗口
    （`-Docx.archive.file.sync.level=0` 时），由快照 + 入站回放兜底重建。
14. **恢复回放依赖 Archive 与媒体驱动在线**：`RecoveryMain` 连接 Archive 控制面
    （`-Dmatch.wal.archive-control-channel`），按 WAL 流 `listRecordings` 后 `replay`
    到恢复流；必须在媒体驱动与 Archive 存活时执行（压测脚本 `--wal-recover-check`
    在停链前调用）。启动引擎前确认 Archive 已启动且 WAL recording 未被清理，
    否则冷启动只能靠快照（或全量重建）。


