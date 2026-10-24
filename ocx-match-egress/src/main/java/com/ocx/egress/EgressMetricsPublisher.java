package com.ocx.egress;

import com.ocx.aeron.AeronWire;
import com.ocx.aeron.OcxAeron;
import com.ocx.outbound.MatchEventSink;

import io.aeron.Publication;

import org.agrona.concurrent.UnsafeBuffer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * egress 指标发布器（2026-08-30，metrics 队列 Aeron 化；Chronicle {@code MetricsQueueAppender}
 * 已删除）：每 2s 把出站发布侧统计（发布字节/背压，egress → Archive）编码为
 * {@code TYPE_OUTBOUND_STATS} 行发布到 metrics(symbol) 流，关键事件（如
 * {@code egress_archive_backpressure}）以 {@code TYPE_EVENT} 行发布到同一流——
 * sidecar 直接订阅 Aeron 消费，不再经 Chronicle 队列文件。
 *
 * <p>禁用形态（{@code -Dmatch.metrics.enabled=false}）：不发布、接口全部 no-op。</p>
 */
public final class EgressMetricsPublisher implements MatchEventSink, OutboundStats {

    private static final long TICK_MS = 2_000L;
    private static final long EVENT_THROTTLE_MS = 1_000L;

    private final Map<String, Publication> publications;
    private final Map<String, byte[]> symbolBytes;
    private final Map<String, AtomicLong> publishedBytes = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> backpressureCount = new ConcurrentHashMap<>();
    private final Map<String, Long> lastEventTs = new ConcurrentHashMap<>();
    private final boolean enabled;

    /** 每线程独立编码缓冲：发布线程与事件发送线程（OutboundSender）互不覆盖。 */
    private final ThreadLocal<UnsafeBuffer> buffer =
            ThreadLocal.withInitial(() -> new UnsafeBuffer(new byte[256]));

    private final Thread thread;
    private volatile boolean running = true;

    public EgressMetricsPublisher(OcxAeron aeron, Collection<String> symbols, boolean enabled) {
        this.enabled = enabled;
        Map<String, Publication> pubs = new HashMap<>(symbols.size());
        Map<String, byte[]> bytes = new HashMap<>(symbols.size());
        for (String symbol : symbols) {
            pubs.put(symbol, aeron.addOutboundMetricsPublication(symbol));
            bytes.put(symbol, AeronWire.symbolBytes(symbol));
        }
        this.publications = pubs;
        this.symbolBytes = bytes;
        this.thread = new Thread(this::run, "egress-metrics-publisher");
        this.thread.setDaemon(true);
    }

    public void start() {
        thread.start();
    }

    public void close() {
        running = false;
        thread.interrupt();
    }

    private void run() {
        while (running) {
            try {
                Thread.sleep(TICK_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            if (!enabled) {
                continue;
            }
            UnsafeBuffer buf = buffer.get();
            for (Map.Entry<String, Publication> e : publications.entrySet()) {
                String symbol = e.getKey();
                AtomicLong bytes = publishedBytes.get(symbol);
                AtomicLong bp = backpressureCount.get(symbol);
                int len =
                        AeronWire.encodeOutboundStats(
                                buf,
                                symbolBytes.get(symbol),
                                bytes == null ? 0L : bytes.get(),
                                bp == null ? 0L : bp.get());
                e.getValue().offer(buf, 0, len);
            }
        }
    }

    @Override
    public void recordPublished(String symbol, long bytes) {
        if (!enabled) {
            return;
        }
        publishedBytes.computeIfAbsent(symbol, s -> new AtomicLong()).addAndGet(bytes);
    }

    @Override
    public void recordBackpressure(String symbol) {
        if (!enabled) {
            return;
        }
        backpressureCount.computeIfAbsent(symbol, s -> new AtomicLong()).incrementAndGet();
    }

    @Override
    public void emit(String symbol, String event, String detail) {
        if (!enabled || !throttle(event)) {
            return;
        }
        Publication pub = publications.get(symbol);
        if (pub == null) {
            return;
        }
        UnsafeBuffer buf = buffer.get();
        pub.offer(buf, 0, AeronWire.encodeEvent(buf, symbolBytes.get(symbol), event, detail));
    }

    @Override
    public void emitAll(String event, String detail) {
        if (!enabled || !throttle(event)) {
            return;
        }
        UnsafeBuffer buf = buffer.get();
        for (Map.Entry<String, Publication> e : publications.entrySet()) {
            e.getValue()
                    .offer(
                            buf,
                            0,
                            AeronWire.encodeEvent(buf, symbolBytes.get(e.getKey()), event, detail));
        }
    }

    /** 同事件名限流：返回 true 表示本次放行（防故障洪峰）。 */
    private boolean throttle(String event) {
        long now = System.currentTimeMillis();
        Long last = lastEventTs.putIfAbsent(event, now);
        if (last == null) {
            return true;
        }
        if (now - last >= EVENT_THROTTLE_MS) {
            lastEventTs.put(event, now);
            return true;
        }
        return false;
    }
}
