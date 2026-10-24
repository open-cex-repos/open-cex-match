package com.ocx.ingress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ocx.aeron.AeronOrderInSink;
import com.ocx.aeron.AeronWire;
import com.ocx.aeron.OcxAeron;
import com.ocx.aeron.StreamIds;
import com.ocx.codec.IoCodec;
import com.ocx.codec.domain.OrderIn;
import com.ocx.codec.domain.OrderInFields;
import com.ocx.config.MutableMatchEngineProperties;
import com.ocx.handler.OrderInSink;
import com.ocx.math.Decimal;
import com.ocx.metrics.MatchMetrics;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * UDP 收单端到端（2026-08-28 入站 Aeron 化）：独立嵌入式驱动 A（生产者）经
 * {@code aeron:udp} 直发 SBE OrderIn 帧 → 独立嵌入式驱动 B（ingress）的
 * {@link AeronOrderInRunner} 收单 → {@link MessageConsumerListener} 校验/路由 →
 * {@link OrderInPublisher} 解码 → {@link AeronOrderInSink} 入站 IPC 环 → core 侧订阅
 * 零拷贝直读字段。全链路不再经过 Kafka。
 */
class AeronUdpIntakeTest {

    @Test
    void orderFlowsFromUdpIntakeThroughIngressPipeline() throws Exception {
        int port = 11_000 + (int) (Math.random() * 20_000);
        String udpEndpoint = "localhost:" + port;
        String symbol = "BTCUSDT";

        // ── ingress 侧：嵌入式媒体驱动 + UDP 订阅 + 收单管线 + IPC 入站环 ──
        OcxAeron ingressAeron = OcxAeron.embedded();
        try (ingressAeron) {
            List<OrderInSink> sinks =
                    List.of(new AeronOrderInSink(ingressAeron.addInboundPublication(symbol)));

            MatchMetrics matchMetrics = new MatchMetrics();
            MutableMatchEngineProperties props = new MutableMatchEngineProperties();
            props.setSymbols(Map.of(symbol, new MutableMatchEngineProperties.CoinScale()));
            OrderInPublisher publisher =
                    new OrderInPublisher(sinks, null, new org.agrona.concurrent.CachedEpochClock());
            MessageConsumerListener listener =
                    new MessageConsumerListener(publisher, matchMetrics, null, props, null);
            listener.initRoutes();
            listener.startSampler(ingressAeron.countersReader());

            Map<String, io.aeron.Subscription> udpSubs = new HashMap<>();
            udpSubs.put(symbol, ingressAeron.addUdpInboundSubscription(udpEndpoint, symbol));
            AeronOrderInRunner runner = new AeronOrderInRunner(udpSubs, listener);
            runner.start();

            // ── core 侧订阅：IPC 入站环直读字段 ──
            var coreSub = ingressAeron.addInboundSubscription(symbol);
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<OrderInFields> received = new AtomicReference<>();

            // ── 生产者：独立嵌入式驱动，UDP 直发 ──
            String producerDir =
                    System.getProperty("java.io.tmpdir")
                            + "/ocx-intake-test-aeron-"
                            + ProcessHandle.current().pid();
            MediaDriver producerDriver =
                    MediaDriver.launch(new MediaDriver.Context().aeronDirectoryName(producerDir));
            Aeron producerAeron =
                    Aeron.connect(new Aeron.Context().aeronDirectoryName(producerDir));
            Publication publication =
                    producerAeron.addPublication(
                            "aeron:udp?endpoint=" + udpEndpoint, StreamIds.inbound(symbol));
            UnsafeBuffer frame = new UnsafeBuffer(new byte[IoCodec.MAX_ORDER_IN_FRAME_LENGTH]);
            try {
                OrderIn order =
                        new OrderIn(
                                777L,
                                888L,
                                symbol,
                                Decimal.parse("70000"),
                                Decimal.parse("1.5"),
                                Decimal.ZERO,
                                null,
                                (byte) 0,
                                (byte) 1,
                                "GTC",
                                (byte) 0,
                                null,
                                null);
                int len = IoCodec.encodeOrderIn(order, frame.byteArray());
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                long result;
                while ((result = publication.offer(frame, 0, len)) < 0) {
                    assertTrue(System.nanoTime() < deadline, "UDP offer 10s 内未成功（背压/未建连）");
                    Thread.sleep(1);
                }

                // ── core 侧轮询断言 ──
                deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (latch.getCount() > 0 && System.nanoTime() < deadline) {
                    coreSub.poll(
                            (buffer, offset, length, header) -> {
                                OrderInFields f = new OrderInFields();
                                IoCodec.readOrderIn(
                                        IoCodec.context(),
                                        buffer,
                                        offset + AeronWire.INBOUND_TS_LENGTH,
                                        AeronWire.INBOUND_FRAME_LENGTH,
                                        f);
                                received.set(f);
                                latch.countDown();
                            },
                            64);
                    Thread.sleep(1);
                }

                assertTrue(latch.getCount() == 0, "IPC 入站环 10s 内未收到 UDP 订单");
                OrderInFields f = received.get();
                assertNotNull(f, "未解码到订单字段");
                assertEquals(777L, f.id);
                assertEquals(888L, f.userId);
                assertEquals(70000L, f.priceMantissa);
                assertEquals(0, f.side);
            } finally {
                publication.close();
                producerAeron.close();
                producerDriver.close();
                runner.close();
            }
        }
    }
}
