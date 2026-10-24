package com.ocx.aeron;

import com.ocx.metrics.MatchMetrics;

import io.aeron.Publication;

import org.agrona.concurrent.UnsafeBuffer;

import java.util.Map;

/**
 * ingress 进程指标发布器（三进程模式）：每 2 秒把入站侧 lag/TPS/拒绝增量编码为
 * INGRESS_METRICS 行发往 core，使 Grafana 的消费积压/TPS/拒绝面板在独立进程形态下仍有值。
 */
public final class IngressMetricsPublisher implements AutoCloseable {

    private static final long TICK_MS = 2_000L;

    private final MatchMetrics matchMetrics;
    private final Map<String, Publication> publications;
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[AeronWire.INGRESS_METRICS_SIZE]);
    private final Thread thread;
    private volatile boolean running = true;

    public IngressMetricsPublisher(
            MatchMetrics matchMetrics, Map<String, Publication> publications) {
        this.matchMetrics = matchMetrics;
        this.publications = publications;
        this.thread = new Thread(this::run, "ingress-metrics-publisher");
        this.thread.setDaemon(true);
    }

    public void start() {
        thread.start();
    }

    private void run() {
        while (running) {
            try {
                Thread.sleep(TICK_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            long rejected = matchMetrics.getRejectedTotal();
            for (Map.Entry<String, Publication> e : publications.entrySet()) {
                int len =
                        AeronWire.encodeIngressMetrics(
                                buffer,
                                matchMetrics.getConsumerLag(),
                                matchMetrics.getCurrentTps(),
                                rejected);
                e.getValue().offer(buffer, 0, len);
            }
        }
    }

    @Override
    public void close() {
        running = false;
        thread.interrupt();
    }
}
