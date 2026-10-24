package com.ocx.aeron;

import static org.junit.jupiter.api.Assertions.*;

import com.ocx.book.MatchFill;
import com.ocx.codec.IoCodec;
import com.ocx.codec.domain.OrderIn;
import com.ocx.codec.domain.OrderInFields;
import com.ocx.math.Decimal;
import com.ocx.metrics.MatchMetrics;
import com.ocx.metrics.MetricsSnapshot;
import com.ocx.outbound.OutboundDepth;

import io.aeron.Publication;
import io.aeron.Subscription;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** 嵌入式媒体驱动下的 IPC 环端到端（入站/出站行编解码 + 发布订阅 + 背压重试）。 */
class AeronTransportTest {

    @Test
    void inboundOrderCrossesIpcRing() throws Exception {
        try (OcxAeron aeron = OcxAeron.embedded()) {
            String symbol = "BTCUSDT";
            var pub = aeron.addInboundPublication(symbol);
            var sub = aeron.addInboundSubscription(symbol);
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<OrderInFields> received = new AtomicReference<>();
            AtomicReference<Long> ts = new AtomicReference<>();

            AeronOrderInSink sink = new AeronOrderInSink(pub);
            OrderIn src =
                    new OrderIn(
                            777L,
                            888L,
                            symbol,
                            Decimal.parse("70000"),
                            Decimal.parse("1.5"),
                            Decimal.ZERO,
                            null,
                            (byte) 1,
                            (byte) 2,
                            "FOK",
                            (byte) 0,
                            null,
                            null);
            byte[] frame = IoCodec.encodeOrderIn(src);
            sink.publish(new UnsafeBuffer(frame), 0, frame.length, 2024L);

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (latch.getCount() > 0 && System.nanoTime() < deadline) {
                sub.poll(
                        (buffer, offset, length, header) -> {
                            OrderInFields f = new OrderInFields();
                            ts.set(AeronWire.readInboundTimestamp(buffer, offset));
                            IoCodec.readOrderIn(
                                    IoCodec.context(),
                                    buffer,
                                    offset + AeronWire.INBOUND_TS_LENGTH,
                                    AeronWire.INBOUND_FRAME_LENGTH,
                                    f);
                            received.set(f);
                            latch.countDown();
                        },
                        10);
                Thread.yield();
            }
            assertEquals(0L, latch.getCount(), "入站 IPC 环未收到消息");
            assertEquals(777L, received.get().id);
            assertEquals(888L, received.get().userId);
            assertEquals(70000L, received.get().priceMantissa);
            assertEquals(1, received.get().side);
            assertEquals(2024L, ts.get());
        }
    }

    @Test
    void outboundRowsCrossIpcRing() throws Exception {
        try (OcxAeron aeron = OcxAeron.embedded()) {
            String symbol = "ETHUSDT";
            var pub = aeron.addOutboundPublication(symbol);
            var sub = aeron.addOutboundSubscription(symbol);

            RecordingHandler handler = new RecordingHandler(2);
            AeronOutboundSource source = new AeronOutboundSource(sub, handler);
            Thread t = new Thread(source);
            t.setDaemon(true);
            t.start();

            AeronFillSink fillSink = new AeronFillSink(pub, symbol, null);
            AeronEventSink eventSink = new AeronEventSink(Map.of(symbol, pub));

            MatchFill fill = new MatchFill();
            fill.symbol = symbol;
            fill.direction = 0;
            fill.takerOrderId = 1L;
            fill.makerOrderId = 2L;
            fill.price = 3500_00000000L;
            fill.priceExponent = -8;
            fill.qty = 10_000000L;
            fill.qtyExponent = -8;
            fill.time = 99L;
            fillSink.submitFills(List.of(fill));

            eventSink.emit(symbol, "wal_append_failed", "type=1 err=boom");

            assertTrue(handler.await(10, TimeUnit.SECONDS), "出站 IPC 环未收到消息");
            assertEquals(1L, handler.fill.takerOrderId);
            assertEquals(2L, handler.fill.makerOrderId);
            assertEquals(3500_00000000L, handler.fill.price);
            assertEquals("wal_append_failed", handler.eventName);
            t.interrupt();
        }
    }

    /** 共享 AeronEventSink 多线程并发 emit：行内容不得互相覆写（线程局部编码缓冲回归）。 */
    @Test
    void concurrentEventEmitsAreNotCorrupted() throws Exception {
        try (OcxAeron aeron = OcxAeron.embedded()) {
            String symbol = "BTCUSDT";
            var pub = aeron.addOutboundPublication(symbol);
            var sub = aeron.addOutboundSubscription(symbol);

            int threads = 4;
            int perThread = 500;
            CountDownLatch received = new CountDownLatch(threads * perThread);
            List<String> names = new ArrayList<>();
            AeronOutboundSource source =
                    new AeronOutboundSource(
                            sub,
                            new OutboundRowHandler() {
                                @Override
                                public void onFill(MatchFill fill) {}

                                @Override
                                public void onDepth(OutboundDepth depth) {}

                                @Override
                                public void onEvent(String symbol, String event, String detail) {
                                    names.add(event);
                                    received.countDown();
                                }

                                @Override
                                public void onMetrics(MetricsSnapshot snapshot) {}
                            });
            Thread t = new Thread(source);
            t.setDaemon(true);
            t.start();

            // 等待发布端与订阅端建连，避免启动期 offer 因 NOT_CONNECTED 丢弃干扰断言
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!pub.isConnected() && System.nanoTime() < deadline) {
                Thread.yield();
            }
            assertTrue(pub.isConnected(), "出站发布端未在 10s 内建连");

            AeronEventSink sink = new AeronEventSink(Map.of(symbol, pub));
            CountDownLatch start = new CountDownLatch(1);
            List<Thread> workers = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                final String evt = "event_" + i;
                Thread w =
                        new Thread(
                                () -> {
                                    try {
                                        start.await();
                                    } catch (InterruptedException ignored) {
                                        return;
                                    }
                                    for (int j = 0; j < perThread; j++) {
                                        sink.emit(symbol, evt, "detail-" + j);
                                    }
                                });
                w.start();
                workers.add(w);
            }
            start.countDown();
            for (Thread w : workers) {
                w.join(10_000);
            }

            assertTrue(received.await(10, TimeUnit.SECONDS), "事件未全部到达");
            assertEquals(threads * perThread, names.size(), "事件行丢失或重复");
            for (int i = 0; i < threads; i++) {
                final String evt = "event_" + i;
                long count = names.stream().filter(n -> n.equals(evt)).count();
                assertEquals(perThread, count, "事件名被并发覆写: " + evt);
            }
            t.interrupt();
        }
    }

    /** 三进程模式的 ingress 指标合流：发布端(lag/TPS/拒绝) → IPC 环 → core 消费端。 */
    @Test
    void ingressMetricsMergeAcrossIpcRing() throws Exception {
        try (OcxAeron aeron = OcxAeron.embedded()) {
            String symbol = "BTCUSDT";
            Map<String, Publication> pubs =
                    Map.of(symbol, aeron.addInboundMetricsPublication(symbol));
            Map<String, Subscription> subs =
                    Map.of(symbol, aeron.addInboundMetricsSubscription(symbol));

            MatchMetrics ingressMetrics = new MatchMetrics();
            ingressMetrics.setConsumerLag(12_345L);
            ingressMetrics.setCurrentTps(512_000.5);
            for (int i = 0; i < 99; i++) {
                ingressMetrics.recordRejectedOrder(symbol, "frame_too_short");
            }

            MatchMetrics coreMetrics = new MatchMetrics();
            IngressMetricsPublisher publisher = new IngressMetricsPublisher(ingressMetrics, pubs);
            IngressMetricsSource source = new IngressMetricsSource(coreMetrics, subs);
            Thread t = new Thread(source, "ingress-metrics-source");
            t.setDaemon(true);
            t.start();
            publisher.start();

            try {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(12);
                while (System.nanoTime() < deadline) {
                    if (coreMetrics.getConsumerLag() == 12_345L
                            && coreMetrics.getCurrentTps() > 512_000.0
                            && coreMetrics.getRejectedTotal() == 99L) {
                        break;
                    }
                    Thread.sleep(100);
                }
                assertEquals(12_345L, coreMetrics.getConsumerLag(), "lag 未合流");
                assertEquals(512_000.5, coreMetrics.getCurrentTps(), 1e-6, "TPS 未合流");
                assertEquals(99L, coreMetrics.getRejectedTotal(), "拒绝总数未合流");
            } finally {
                publisher.close();
                source.stop();
                t.interrupt();
            }
        }
    }

    /** WAL 批量帧：混合类型攒批 → 独立 WAL 持久化流 → 直连订阅拆批，保序逐条回调。 */
    private static final class RecordingHandler implements OutboundRowHandler {

        private final CountDownLatch latch;
        MatchFill fill;
        String eventName;

        RecordingHandler(int expected) {
            this.latch = new CountDownLatch(expected);
        }

        boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            return latch.await(timeout, unit);
        }

        @Override
        public void onFill(MatchFill fill) {
            this.fill = fill;
            latch.countDown();
        }

        @Override
        public void onDepth(OutboundDepth depth) {}

        @Override
        public void onEvent(String symbol, String event, String detail) {
            this.eventName = event;
            latch.countDown();
        }

        @Override
        public void onMetrics(MetricsSnapshot snapshot) {}
    }
}
