package com.ocx.egress;

import com.ocx.aeron.AeronOutboundSource;
import com.ocx.aeron.ControlStream;
import com.ocx.aeron.OcxAeron;
import com.ocx.config.ConfigLoader;
import com.ocx.config.MatchEngineConfig;

import io.aeron.Subscription;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * 出站进程入口：订阅 core 的出站 IPC 环 → Aeron Archive 发布；指标经 metrics 流
 * 直发 sidecar（2026-08-30，Chronicle metrics 队列已删）。
 * WAL 持久化自 2026-08-28 起由 core 直发 Archive 记录流（本进程不再写 WAL）。
 * 依赖独立媒体驱动进程（{@code -Docx.aeron.dir=}，默认 /tmp/ocx-aeron）。
 */
public final class MatchEgressBootstrap {

    private static final CountDownLatch LATCH = new CountDownLatch(1);

    private final MatchEngineConfig cfg;
    private final List<AeronOutboundSource> sources = new ArrayList<>();
    private final List<Thread> threads = new ArrayList<>();

    private MatchEgressBootstrap(MatchEngineConfig cfg) {
        this.cfg = cfg;
    }

    public static void main(String[] args) throws InterruptedException {
        java.util.logging.LogManager.getLogManager().reset();
        System.setProperty("org.jboss.logging.provider", "slf4j");
        MatchEngineConfig config = ConfigLoader.load(args);
        MatchEgressBootstrap bootstrap = new MatchEgressBootstrap(config);
        bootstrap.start();
        System.err.println(
                "[MatchEgressBootstrap] MATCH_EGRESS_READY profile="
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
                                "egress-shutdown-hook"));
        LATCH.await();
    }

    public void start() {
        Map<String, com.ocx.config.MatchEngineProperties.CoinScale> symbols =
                cfg.getMatchEngineProperties().getSymbols();

        OcxAeron aeron = OcxAeron.connect();
        try {
            // 指标 Aeron 化（2026-08-30）：egress 只发布出站统计/事件到 metrics 流，
            // sidecar 直接订阅；Chronicle MetricsQueueAppender 已删除。
            EgressMetricsPublisher metricsPublisher =
                    new EgressMetricsPublisher(aeron, symbols.keySet(), cfg.isMetricsEnabled());
            metricsPublisher.start();
            OutboundSender outbound =
                    new OutboundSender(aeron, symbols.keySet(), metricsPublisher, metricsPublisher);
            outbound.start();
            for (String symbol : symbols.keySet()) {
                Subscription subscription = aeron.addOutboundSubscription(symbol);
                AeronEgressHandler handler = new AeronEgressHandler(outbound);
                AeronOutboundSource source = new AeronOutboundSource(subscription, symbol, handler);
                sources.add(source);
                Thread t = new Thread(source, "egress-" + symbol);
                t.setDaemon(true);
                t.start();
                threads.add(t);
            }
            // 控制流 READY（编排侧 wait-ready；尽力而为，不阻塞启动）
            ControlStream.publishReady(aeron.aeronDirectoryName(), "egress");
        } catch (RuntimeException e) {
            aeron.close();
            throw e;
        }
        // aeron 生命周期随 JVM 关闭钩子结束（stop 中关闭）。
    }

    public void stop() {
        for (AeronOutboundSource s : sources) {
            s.stop();
        }
        for (Thread t : threads) {
            t.interrupt();
        }
    }
}
