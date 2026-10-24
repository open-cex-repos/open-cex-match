package com.ocx.ingress;

import com.ocx.aeron.AeronWire;
import com.ocx.aeron.OcxAeron;
import com.ocx.aeron.StreamIds;

import org.agrona.concurrent.status.CountersReader;

import java.util.Collection;

/**
 * 入站积压探针（Aeron 版，替代已删除的 Kafka {@code ConsumerLagProbe}）。
 *
 * <p>语义从「Kafka 消费积压」迁移为「入站环积压」：采样 ingress→core IPC 入站环的
 * 发布位置 − 订阅位置（消息数）。收单改成 Aeron 后，生产端没有可堆积的无界日志，
 * 机器内部真正会堆积的队列就是入站环——core 消费变慢 → 环积压上升 → ingress offer
 * 阻塞 → UDP 不再 poll → 媒体驱动缓冲满 → 生产端退避，整条背压链以它为首发信号。
 * 两个位置计数器都在共享媒体驱动内，ingress 侧可直接读取；Grafana 指标名
 * {@code match_consumer_lag} 保持不变。</p>
 */
final class AeronIntakeLagProbe {

    /** 发布端位置计数器类型（Aeron 1.52 PublisherPos，与 AeronMetricsPublisher 实测一致）。 */
    private static final int PUB_POSITION_TYPE_ID = 12;

    /** 订阅端位置计数器类型（Aeron 1.52 SubscriberPos）。 */
    private static final int SUB_POSITION_TYPE_ID = 4;

    private final CountersReader counters;
    private final Collection<String> symbols;

    AeronIntakeLagProbe(CountersReader counters, Collection<String> symbols) {
        this.counters = counters;
        this.symbols = symbols;
    }

    /** 全 symbol 入站环积压（消息数）。 */
    long totalBacklog() {
        long total = 0L;
        for (String symbol : symbols) {
            long sub = position(SUB_POSITION_TYPE_ID, symbol);
            long pub = position(PUB_POSITION_TYPE_ID, symbol);
            total += Math.max(0L, (pub - sub) / AeronWire.INBOUND_ROW_SIZE);
        }
        return total;
    }

    /** 按类型 + label 尾（"<streamId> aeron:ipc"）定位入站环的位置计数器。 */
    private long position(int typeId, String symbol) {
        String suffix = StreamIds.inbound(symbol) + " " + OcxAeron.CHANNEL;
        final long[] found = {0L};
        counters.forEach(
                (id, type, label) -> {
                    if (found[0] == 0L && type == typeId && label.endsWith(suffix)) {
                        found[0] = counters.getCounterValue((int) id);
                    }
                });
        return found[0];
    }
}
