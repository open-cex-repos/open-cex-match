package com.ocx;

import com.ocx.aeron.AeronEventSink;
import com.ocx.aeron.AeronFillSink;
import com.ocx.aeron.AeronMetricsPublisher;
import com.ocx.aeron.AeronOrderInSource;
import com.ocx.aeron.ControlStream;
import com.ocx.aeron.IngressMetricsSource;
import com.ocx.aeron.OcxAeron;
import com.ocx.book.OrderBook;
import com.ocx.book.TriggerSlots;
import com.ocx.config.ConfigLoader;
import com.ocx.config.MatchEngineConfig;
import com.ocx.config.MatchEngineProperties;
import com.ocx.engine.MatchService;
import com.ocx.ha.HaHeartbeat;
import com.ocx.handler.OrderEventHandler;
import com.ocx.metrics.MatchMetrics;
import com.ocx.outbound.FillSink;
import com.ocx.outbound.MatchEventSink;
import com.ocx.recovery.RecoveryService;
import com.ocx.snapshot.SnapshotCoordinator;
import com.ocx.snapshot.SnapshotWriter;

import io.aeron.Publication;
import io.aeron.Subscription;

import net.openhft.affinity.AffinityThreadFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.function.LongSupplier;

/**
 * 撮合核心进程入口：订阅入站 IPC 环 → 纵表撮合 → 发布出站 IPC 环 + WAL 持久化流。
 * 无 Kafka、无 SBE 解码、无磁盘 IO（WAL 由 Archive 记录）；依赖独立媒体驱动进程。
 */
public final class MatchCoreBootstrap {

    private static final CountDownLatch LATCH = new CountDownLatch(1);

    private final MatchEngineConfig cfg;
    private final List<AeronOrderInSource> sources = new ArrayList<>();
    private final List<Thread> threads = new ArrayList<>();
    private AeronMetricsPublisher metricsPublisher;
    private IngressMetricsSource ingressMetricsSource;
    private SnapshotWriter snapshotWriter;
    private HaHeartbeat haHeartbeat;
    private OcxAeron aeron;

    private MatchCoreBootstrap(MatchEngineConfig cfg) {
        this.cfg = cfg;
    }

    public static void main(String[] args) throws InterruptedException {
        java.util.logging.LogManager.getLogManager().reset();
        System.setProperty("org.jboss.logging.provider", "slf4j");
        MatchEngineConfig config = ConfigLoader.load(args);
        MatchCoreBootstrap bootstrap = new MatchCoreBootstrap(config);
        bootstrap.start();
        System.err.println(
                "[MatchCoreBootstrap] MATCH_CORE_READY profile="
                        + config.getProfile()
                        + " symbols="
                        + config.getMatchEngineProperties().getSymbols().keySet());
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    bootstrap.stop();
                                    LATCH.countDown();
                                },
                                "core-shutdown-hook"));
        LATCH.await();
    }

    public void start() {
        MatchEngineProperties props = cfg.getMatchEngineProperties();
        MatchMetrics matchMetrics = new MatchMetrics(MatchMetrics.modeFromProperty());
        aeron = OcxAeron.connect();

        Map<String, Publication> outboundPubs = new HashMap<>();
        Map<String, Publication> metricsPubs = new HashMap<>();
        for (String symbol : props.getSymbols().keySet()) {
            outboundPubs.put(symbol, aeron.addOutboundPublication(symbol));
            metricsPubs.put(symbol, aeron.addOutboundMetricsPublication(symbol));
        }
        // 关键事件直发 metrics 流（2026-08-30）：sidecar 直接订阅，不再经出站业务环/Chronicle
        MatchEventSink eventSink = new AeronEventSink(metricsPubs);

        // 每 symbol：订单簿 + 处理器（成交/深度/事件/状态变更全部走出站环）
        Map<String, OrderBook> bookMap = new HashMap<>();
        Map<String, OrderEventHandler> handlers = new HashMap<>();
        for (Map.Entry<String, MatchEngineProperties.CoinScale> entry :
                props.getSymbols().entrySet()) {
            String symbol = entry.getKey();
            MatchEngineProperties.CoinScale value = entry.getValue();
            int coinScale =
                    value != null ? value.getCoinScale() : props.getDefaults().getCoinScale();
            int baseCoinScale =
                    value != null
                            ? value.getBaseCoinScale()
                            : props.getDefaults().getBaseCoinScale();
            long cap = props.getMaxOpenOrders() > 0 ? props.getMaxOpenOrders() : 1_000_000L;
            int orderCapacity = (int) Math.min(cap, 1 << 30);
            int levelCapacity = (int) Math.min(Math.max(cap / 16, 1024), 1 << 20);

            Publication outPub = outboundPubs.get(symbol);
            FillSink fillSink = new AeronFillSink(outPub, symbol, matchMetrics);
            OrderBook orderBook =
                    new OrderBook(
                            symbol, coinScale, baseCoinScale, orderCapacity, levelCapacity, cap);
            matchMetrics.registerOrderBook(orderBook);
            bookMap.put(symbol, orderBook);
            // 触发价档位容量 65536：500K 混合单下条件单触发价随 mid 漂移产生数万不同价位，
            // 4096 会耗尽并抛异常（已改为容量满拒绝）；65536 ≈ 8 表 × 47MB，覆盖 stress 场景
            TriggerSlots triggers =
                    new TriggerSlots((int) Math.min(Math.max(cap / 4, 1024), 1 << 20), 1 << 16);
            OrderEventHandler handler =
                    new OrderEventHandler(
                            orderBook,
                            triggers,
                            new MatchService(cfg.isSelfTradePrevention()),
                            fillSink,
                            matchMetrics,
                            eventSink);
            handler.setDepthPublishIntervalMs(cfg.getDepthPublishIntervalMs());
            handlers.put(symbol, handler);
        }

        // 快照 + 入站 recording 增量回放（输入 journal：Archive 记录入站流，此处重跑撮合重建簿状态）
        new RecoveryService(eventSink)
                .recover(cfg.getRecoveryConfig(), aeron.aeronDirectoryName(), handlers);

        // 快照线程（Aeron Archive recording 快照：全局单线程编码/发布/回收，见 SnapshotWriter）
        snapshotWriter =
                new SnapshotWriter(
                        aeron, cfg.getRecoveryConfig(), aeron.aeronDirectoryName(), eventSink);

        // 入站订阅线程（每 symbol 一条，绑定独立核）
        // 撮合线程工厂：默认绑核；-Docx.match.affinity=false 关闭（对照实验用）
        ThreadFactory tf =
                "false".equalsIgnoreCase(System.getProperty("ocx.match.affinity", "true"))
                        ? Thread::new
                        : new AffinityThreadFactory("Match-Handler-");
        Map<String, Subscription> inboundSubs = new HashMap<>();
        for (String symbol : handlers.keySet()) {
            Subscription subscription = aeron.addInboundSubscription(symbol);
            inboundSubs.put(symbol, subscription);
            AeronOrderInSource source =
                    new AeronOrderInSource(subscription, handlers.get(symbol), symbol);
            sources.add(source);
            long snapInterval = Long.getLong("ocx.snapshot.interval-ms", 10_000L);
            SnapshotWriter.SymbolSink snapSink = snapshotWriter.register(symbol);
            boolean journalBridgeEnabled =
                    Boolean.parseBoolean(System.getProperty("ocx.journal-bridge.enabled", "false"));
            java.util.function.Supplier<long[]> journalDurability =
                    journalBridgeEnabled
                            ? () ->
                                    com.ocx.aeron.JournalDurability.read(
                                            aeron.countersReader(), symbol)
                            : null;
            handlers.get(symbol)
                    .setSnapshotCoordinator(
                            new SnapshotCoordinator(
                                    bookMap.get(symbol),
                                    handlers.get(symbol).getStops(),
                                    source::imagePositions,
                                    snapInterval,
                                    snapSink,
                                    journalDurability));
            Thread t = tf.newThread(source);
            t.setName("Match-Handler-" + symbol + ":");
            t.setDaemon(true);
            t.start();
            threads.add(t);
        }
        snapshotWriter.start();

        metricsPublisher =
                new AeronMetricsPublisher(
                        matchMetrics,
                        metricsPubs,
                        inboundSubs,
                        aeron.countersReader(),
                        cfg.getMetricsLagTrigger(),
                        cfg.getMetricsLagRecover());

        // 主备心跳（阶段 B）：-Docx.ha.enabled / -Docx.ha.instance-id / -Docx.ha.role
        boolean haEnabled = Boolean.parseBoolean(System.getProperty("ocx.ha.enabled", "true"));
        if (haEnabled) {
            int instanceId = Integer.getInteger("ocx.ha.instance-id", 1);
            String role = System.getProperty("ocx.ha.role", "primary");
            // 仲裁层（Ratis 3 节点组）：心跳发给全部协调节点
            // （-Docx.ha.udp.endpoints 逗号分隔）；单协调者兼容 -Docx.ha.udp.endpoint
            List<String> haChannels = new ArrayList<>();
            String endpointsSpec = System.getProperty("ocx.ha.udp.endpoints");
            if (endpointsSpec != null && !endpointsSpec.isBlank()) {
                for (String part : endpointsSpec.split(",")) {
                    haChannels.add(part.trim());
                }
            } else {
                haChannels.add(
                        System.getProperty(
                                "ocx.ha.udp.endpoint", "aeron:udp?endpoint=localhost:53000"));
            }
            String haListen =
                    System.getProperty("ocx.ha.udp.listen", "aeron:udp?endpoint=0.0.0.0:53000");
            // 备机上报复制位置（standbyInbound 入站消费进度）→ 心跳 position 字段 →
            // 仲裁组成员表实时更新 → 提升候选按"追得最齐"选优（P0-1）
            LongSupplier positionSupplier = null;
            if (Boolean.parseBoolean(System.getProperty("ocx.standby.inbound-stream", "false"))) {
                positionSupplier =
                        () -> {
                            long maxPos = 0L;
                            for (AeronOrderInSource s : sources) {
                                for (long p : s.imagePositions().values()) {
                                    maxPos = Math.max(maxPos, p);
                                }
                            }
                            return maxPos;
                        };
            }
            // self-fencing（防双主）：租约吊销/协调者失联 → 停止撮合线程（fail-stop）
            haHeartbeat =
                    new HaHeartbeat(
                            aeron.aeronDirectoryName(),
                            haChannels,
                            haListen,
                            instanceId,
                            role,
                            () -> {
                                System.err.println(
                                        "[MatchCoreBootstrap] FENCED instanceId="
                                                + instanceId
                                                + "：停止全部撮合线程（fail-stop）");
                                for (AeronOrderInSource s : sources) {
                                    s.stop();
                                }
                            },
                            positionSupplier);
            Thread heartbeatThread = new Thread(haHeartbeat, "ha-heartbeat");
            heartbeatThread.setDaemon(true);
            heartbeatThread.start();
        }
        metricsPublisher.start();

        // 三进程模式下消费 ingress 指标行（lag/TPS/拒绝合流进核心快照）
        Map<String, Subscription> ingressMetricsSubs = new HashMap<>();
        for (String symbol : handlers.keySet()) {
            ingressMetricsSubs.put(symbol, aeron.addInboundMetricsSubscription(symbol));
        }
        ingressMetricsSource = new IngressMetricsSource(matchMetrics, ingressMetricsSubs);
        Thread ingressMetricsThread = new Thread(ingressMetricsSource, "ingress-metrics-source");
        ingressMetricsThread.setDaemon(true);
        ingressMetricsThread.start();
        threads.add(ingressMetricsThread);

        // 控制流 READY（编排侧 wait-ready；尽力而为，不阻塞启动）
        ControlStream.publishReady(aeron.aeronDirectoryName(), "core");
    }

    public void stop() {
        if (ingressMetricsSource != null) {
            ingressMetricsSource.stop();
        }
        for (AeronOrderInSource s : sources) {
            s.stop();
        }
        for (Thread t : threads) {
            t.interrupt();
        }
        if (metricsPublisher != null) {
            metricsPublisher.close();
        }
        if (snapshotWriter != null) {
            snapshotWriter.close();
        }
        if (haHeartbeat != null) {
            haHeartbeat.stop();
        }
        if (aeron != null) {
            aeron.close();
        }
    }
}
