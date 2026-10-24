package com.ocx.aeron;

import com.ocx.metrics.MatchMetrics;
import com.ocx.metrics.MetricsSnapshot;

import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;

import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.util.List;
import java.util.Map;

/**
 * 指标快照发布器（core 侧）：每 5 秒把 MatchMetrics 快照编码为 METRICS 行
 * 经出站环交给 egress 写 metrics 队列。单次 offer、失败跳过（下一拍补上），
 * 不与撮合热路径争抢背压。
 */
public final class AeronMetricsPublisher implements AutoCloseable {

    /** 指标快照周期 5s：HdrHistogram interval 轮换/汇报不再是业务累赘（2s→5s，JFR 卡点优化）。 */
    private static final long TICK_MS = 5_000L;

    /** 发布端位置计数器类型（实测 1.52.2 为 type=49 "pub-pos (concurrent)"；label 以 "<streamId> aeron:ipc" 结尾）。 */
    private static final int PUBLICATION_POSITION_TYPE_ID = 49;

    private final MatchMetrics matchMetrics;
    private final Map<String, Publication> publications;

    /** 每 symbol 入站订阅（采样"入站环积压"：availablePosition - position，按消息数）。 */
    private final Map<String, Subscription> inboundSubscriptions;

    private final CountersReader counters;

    /** symbol → 16 字节预编码。 */
    private final Map<String, byte[]> symbolBytes;

    private final long lagTrigger;
    private final long lagRecover;
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[AeronWire.METRICS_SIZE]);
    private final Thread thread;
    private volatile boolean running = true;
    private volatile boolean overloaded = false;

    public AeronMetricsPublisher(
            MatchMetrics matchMetrics,
            Map<String, Publication> publications,
            Map<String, Subscription> inboundSubscriptions,
            CountersReader counters,
            long lagTrigger,
            long lagRecover) {
        this.matchMetrics = matchMetrics;
        this.publications = publications;
        this.inboundSubscriptions = inboundSubscriptions;
        this.counters = counters;
        Map<String, byte[]> bytes = new java.util.HashMap<>(publications.size());
        for (String s : publications.keySet()) {
            bytes.put(s, AeronWire.symbolBytes(s));
        }
        this.symbolBytes = bytes;
        this.lagTrigger = lagTrigger;
        this.lagRecover = lagRecover;
        this.thread = new Thread(this::run, "match-metrics-publisher");
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
            updateOverloaded();
            sampleInboundBacklog();
            for (Map.Entry<String, Publication> e : publications.entrySet()) {
                MetricsSnapshot snapshot = matchMetrics.snapshot(e.getKey(), overloaded);
                int len = AeronWire.encodeMetrics(buffer, snapshot, symbolBytes.get(e.getKey()));
                e.getValue().offer(buffer, 0, len);
            }
        }
    }

    /** 入站环积压采样：发布端位置计数器 − 订阅端位置计数器（字节）÷ 128B = 未消费消息数。 */
    private void sampleInboundBacklog() {
        for (Map.Entry<String, Subscription> e : inboundSubscriptions.entrySet()) {
            long messages = 0L;
            Subscription sub = e.getValue();
            List<Image> images = sub.images();
            if (images != null && !images.isEmpty()) {
                Image image = images.get(0);
                int subCounterId = image.subscriberPositionId();
                if (subCounterId != -1) {
                    long subPos = counters.getCounterValue(subCounterId);
                    long pubPos = publicationPosition(StreamIds.inbound(e.getKey()));
                    messages = Math.max(0L, (pubPos - subPos) / AeronWire.INBOUND_ROW_SIZE);
                }
            }
            matchMetrics.setInboundBacklog(e.getKey(), messages);
        }
    }

    /** 查找指定入站流的发布端位置计数器（label 尾 = "<streamId> aeron:ipc"）。 */
    private long publicationPosition(int streamId) {
        final long[] found = {0L};
        counters.forEach(
                (id, typeId, label) -> {
                    if (found[0] == 0L
                            && typeId == PUBLICATION_POSITION_TYPE_ID
                            && label.endsWith(streamId + " " + OcxAeron.CHANNEL)) {
                        found[0] = counters.getCounterValue((int) id);
                    }
                });
        return found[0];
    }

    /** 实例级过载判断 + 滞回（与 sidecar 判活同语义，仅依赖消费积压）。 */
    private void updateOverloaded() {
        long lag = matchMetrics.getConsumerLag();
        boolean lagOver = lag > lagTrigger;
        if (lagOver) {
            overloaded = true;
        } else if (overloaded) {
            boolean lagRecovered = lag < lagRecover;
            if (lagRecovered) {
                overloaded = false;
            }
        }
    }

    @Override
    public void close() {
        running = false;
        thread.interrupt();
    }
}
