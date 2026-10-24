package com.ocx.aeron;

import com.ocx.metrics.MatchMetrics;

import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;

import java.util.Map;

/**
 * core 进程消费 ingress 指标行（三进程模式）：把入站侧 lag/TPS/拒绝增量合并进核心
 * MatchMetrics，随核心 METRICS 行进入 Grafana。
 */
public final class IngressMetricsSource implements Runnable {

    private static final int FRAGMENT_LIMIT = 16;

    private final MatchMetrics matchMetrics;
    private final Map<String, Subscription> subscriptions;

    /** 方法引用提升为字段：poll 每轮调用零分配（同 AeronOrderInSource 修复）。 */
    private final FragmentHandler fragmentHandler = this::onFragment;

    /** 指标流 2s 低频、非延迟关键：固定毫秒睡眠，空闲几乎零 CPU。 */
    private final IdleStrategy idle = new SleepingMillisIdleStrategy(1);

    /** 已并入核心的 ingress 拒绝总数（绝对值对比，多 symbol 行去重）。 */
    private long lastRejected;

    private volatile boolean running = true;

    public IngressMetricsSource(
            MatchMetrics matchMetrics, Map<String, Subscription> subscriptions) {
        this.matchMetrics = matchMetrics;
        this.subscriptions = subscriptions;
    }

    @Override
    public void run() {
        while (running) {
            int total = 0;
            for (Subscription sub : subscriptions.values()) {
                total += sub.poll(fragmentHandler, FRAGMENT_LIMIT);
            }
            if (total == 0) {
                idle.idle();
            }
        }
    }

    private void onFragment(
            DirectBuffer buffer, int offset, int length, io.aeron.logbuffer.Header header) {
        if (buffer.getByte(offset) == AeronWire.TYPE_INGRESS_METRICS) {
            AeronWire.IngressMetricsRow row = AeronWire.decodeIngressMetrics(buffer, offset);
            matchMetrics.setConsumerLag(row.consumerLag());
            matchMetrics.setCurrentTps(row.currentTps());
            long total = row.rejectedTotal();
            if (total > lastRejected) {
                matchMetrics.addRejectedDelta(total - lastRejected);
                lastRejected = total;
            }
        }
    }

    public void stop() {
        running = false;
    }
}
