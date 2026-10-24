package com.ocx.ingress;

import com.ocx.aeron.AeronOrderInSink;
import com.ocx.aeron.ControlStream;
import com.ocx.aeron.InboundJournalBridge;
import com.ocx.aeron.IngressMetricsPublisher;
import com.ocx.aeron.OcxAeron;
import com.ocx.config.ConfigLoader;
import com.ocx.config.MatchEngineConfig;
import com.ocx.handler.OrderInSink;
import com.ocx.metrics.MatchMetrics;

import io.aeron.Publication;
import io.aeron.Subscription;

import org.agrona.concurrent.CachedEpochClock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * 入站进程入口：Aeron UDP 收单 → 校验 → SBE 解码 → 发布到入站 IPC 环。
 * 依赖独立媒体驱动进程（{@code -Docx.aeron.dir=}，默认 /tmp/ocx-aeron）。
 * UDP 收单端点由 {@code -Docx.ingress.aeron.udp.endpoint=} 指定（默认 0.0.0.0:11000）。
 */
public final class MatchIngressBootstrap {

    private static final CountDownLatch LATCH = new CountDownLatch(1);

    /** UDP 收单端点（客户端/网关直发 SBE OrderIn 帧），默认 0.0.0.0:11000。 */
    static final String DEFAULT_UDP_ENDPOINT = "0.0.0.0:11000";

    private final MatchEngineConfig cfg;
    private AeronOrderInRunner orderInRunner;
    private AeronDeadLetterSink dltSink;
    private OcxAeron aeron;
    private IngressMetricsPublisher ingressMetricsPublisher;
    private final List<InboundJournalBridge> journalBridges = new ArrayList<>();

    private MatchIngressBootstrap(MatchEngineConfig cfg) {
        this.cfg = cfg;
    }

    public static void main(String[] args) throws InterruptedException {
        java.util.logging.LogManager.getLogManager().reset();
        System.setProperty("org.jboss.logging.provider", "slf4j");
        MatchEngineConfig config = ConfigLoader.load(args);
        MatchIngressBootstrap bootstrap = new MatchIngressBootstrap(config);
        bootstrap.start();
        System.err.println(
                "[MatchIngressBootstrap] MATCH_INGRESS_READY profile="
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
                                "ingress-shutdown-hook"));
        LATCH.await();
    }

    public void start() {
        MatchMetrics matchMetrics = new MatchMetrics();
        aeron = OcxAeron.connect();
        // 槽位数组与路由层 SymbolRoute 索引一一对应（同一 keySet 迭代顺序）
        List<OrderInSink> sinks = new ArrayList<>();
        Map<String, Publication> metricsPubs = new HashMap<>();
        Map<String, Subscription> udpSubs = new HashMap<>();
        // 录制解耦（2026-09-01）：ingress 只发 feed 流，journal bridge 转发 live/journal。
        // 默认关闭（兼容），压测/生产编排侧显式 -Docx.journal-bridge.enabled=true。
        boolean journalBridgeEnabled =
                Boolean.parseBoolean(System.getProperty("ocx.journal-bridge.enabled", "false"));
        int ringSize = Integer.getInteger("ocx.journal-bridge.ring-size", 1 << 28);
        String udpEndpoint =
                System.getProperty("ocx.ingress.aeron.udp.endpoint", DEFAULT_UDP_ENDPOINT);
        for (String symbol : cfg.getMatchEngineProperties().getSymbols().keySet()) {
            if (journalBridgeEnabled) {
                Publication feedPub = aeron.addInboundFeedPublication(symbol);
                sinks.add(new AeronOrderInSink(feedPub));
                journalBridges.add(
                        new InboundJournalBridge(
                                aeron.aeron(),
                                aeron.addInboundFeedSubscription(symbol),
                                aeron.addInboundPublication(symbol),
                                aeron.addInboundJournalPublication(symbol),
                                symbol,
                                ringSize));
            } else {
                Publication pub = aeron.addInboundPublication(symbol);
                sinks.add(new AeronOrderInSink(pub));
            }
            metricsPubs.put(symbol, aeron.addInboundMetricsPublication(symbol));
            udpSubs.put(symbol, aeron.addUdpInboundSubscription(udpEndpoint, symbol));
        }

        // 收单时刻缓存时钟（E10：逐帧 System.currentTimeMillis → 每 poll 批一次）
        CachedEpochClock clock = new CachedEpochClock();
        clock.update(System.currentTimeMillis());
        OrderInPublisher publisher = new OrderInPublisher(sinks, null, clock);
        dltSink = new AeronDeadLetterSink(aeron);
        MessageConsumerListener listener =
                new MessageConsumerListener(
                        publisher, matchMetrics, dltSink, cfg.getMatchEngineProperties(), null);
        listener.initRoutes();
        listener.startSampler(aeron.countersReader());

        orderInRunner = new AeronOrderInRunner(udpSubs, listener, clock);
        orderInRunner.start();
        for (InboundJournalBridge bridge : journalBridges) {
            bridge.start();
        }

        // 入站侧 lag/TPS/拒绝 → core 指标合流（三进程模式下 Grafana 面板有值）
        ingressMetricsPublisher = new IngressMetricsPublisher(matchMetrics, metricsPubs);
        ingressMetricsPublisher.start();

        // 控制流 READY（编排侧 wait-ready；尽力而为，不阻塞启动）
        ControlStream.publishReady(aeron.aeronDirectoryName(), "ingress");
    }

    public void stop() {
        for (InboundJournalBridge bridge : journalBridges) {
            bridge.close();
        }
        if (ingressMetricsPublisher != null) {
            ingressMetricsPublisher.close();
        }
        if (orderInRunner != null) {
            orderInRunner.close();
        }
        if (aeron != null) {
            aeron.close();
        }
        if (dltSink != null) {
            dltSink.close();
        }
    }
}
