package com.ocx.metrics;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ocx.aeron.AeronWire;
import com.ocx.aeron.OcxAeron;

import io.aeron.Publication;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * metrics 流 Aeron 订阅端回环（2026-08-30，Chronicle 队列删除）：内嵌媒体驱动 +
 * 发布 METRICS / OUTBOUND_STATS / EVENT 行 → poll → 渲染 Prometheus 文本断言。
 */
class MetricsAeronSourceTest {

    private static final String SYMBOL = "BTCUSDT";

    @Test
    void 订阅metrics流并渲染Prometheus文本() throws Exception {
        try (OcxAeron aeron = OcxAeron.embedded()) {
            Publication pub = aeron.addOutboundMetricsPublication(SYMBOL);
            MetricsAeronSource source =
                    new MetricsAeronSource(aeron.aeronDirectoryName(), SYMBOL, "spot", 10);
            source.start();
            source.ensureConnected();
            try {
                // Aeron 订阅异步注册：等待 image 建立后再发布，避免首帧落在订阅生效前
                Thread.sleep(300);
                MetricsSnapshot snapshot =
                        new MetricsSnapshot(
                                SYMBOL,
                                1000L,
                                900L,
                                false,
                                42L,
                                60000L,
                                123.0,
                                10L,
                                2L,
                                8L,
                                1L,
                                1L,
                                5L,
                                100000L,
                                3,
                                4,
                                1.0,
                                2.0,
                                3.0,
                                4.0,
                                5.0,
                                64L,
                                new JvmMetrics(
                                        256L, 512L, 1024L, 64L, 12L, 8L, 20L, 3L, 15L, 5L, 60000L),
                                5L,
                                9L,
                                3L,
                                2L);
                UnsafeBuffer buf = new UnsafeBuffer(new byte[AeronWire.METRICS_SIZE]);
                int len = AeronWire.encodeMetrics(buf, snapshot, AeronWire.symbolBytes(SYMBOL));
                while (pub.offer(buf, 0, len) < 0L) {
                    Thread.onSpinWait();
                }

                UnsafeBuffer stats = new UnsafeBuffer(new byte[AeronWire.OUTBOUND_STATS_SIZE]);
                len = AeronWire.encodeOutboundStats(stats, AeronWire.symbolBytes(SYMBOL), 999L, 7L);
                while (pub.offer(stats, 0, len) < 0L) {
                    Thread.onSpinWait();
                }

                UnsafeBuffer ev = new UnsafeBuffer(new byte[256]);
                len =
                        AeronWire.encodeEvent(
                                ev, AeronWire.symbolBytes(SYMBOL), "purge_segments", "ok");
                while (pub.offer(ev, 0, len) < 0L) {
                    Thread.onSpinWait();
                }

                source.poll();
                String out = source.render();
                assertTrue(out.contains("match_engine_up{symbol=\"BTCUSDT\",profile=\"spot\"} 1"));
                assertTrue(
                        out.contains("match_consumer_lag{symbol=\"BTCUSDT\",profile=\"spot\"} 42"));
                assertTrue(
                        out.contains("match_trades_total{symbol=\"BTCUSDT\",profile=\"spot\"} 8"));
                assertTrue(
                        out.contains(
                                "match_outbound_published_bytes_total{symbol=\"BTCUSDT\",profile=\"spot\"}"
                                    + " 999"));
                assertTrue(
                        out.contains(
                                "match_egress_backpressure_total{symbol=\"BTCUSDT\",profile=\"spot\"}"
                                    + " 7"));
                assertTrue(
                        out.contains(
                                "match_order_book_depth{symbol=\"BTCUSDT\",profile=\"spot\",side=\"bid\"}"
                                    + " 3"));
                assertTrue(
                        out.contains(
                                "jvm_memory_used_bytes{symbol=\"BTCUSDT\",profile=\"spot\",area=\"heap\"}"
                                    + " 256"));
                assertTrue(
                        out.contains(
                                "match_engine_events_total{symbol=\"BTCUSDT\",profile=\"spot\",event=\"purge_segments\"}"
                                    + " 1"));
            } finally {
                source.stop();
            }
        }
    }
}
