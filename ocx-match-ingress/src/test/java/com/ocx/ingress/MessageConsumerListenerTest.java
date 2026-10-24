package com.ocx.ingress;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.ocx.codec.IoCodec;
import com.ocx.codec.domain.OrderIn;
import com.ocx.config.MutableMatchEngineProperties;
import com.ocx.math.Decimal;
import com.ocx.metrics.MatchMetrics;
import com.ocx.outbound.DeadLetterSink;

import org.agrona.DirectBuffer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * 入站 SBE 直通测试：消费线程只做校验 + 定长 symbol 路由 + memcpy 发布，
 * 不再解析字段/构造 Order；非 SBE 或未知 symbol 拒绝并进 DLT。
 */
class MessageConsumerListenerTest {

    @Test
    void sbePayloadDecodesAndDispatches() {
        OrderInPublisher orderInPublisher = mock(OrderInPublisher.class);
        MatchMetrics matchMetrics = mock(MatchMetrics.class);
        MessageConsumerListener listener = newListener(orderInPublisher, matchMetrics);

        OrderIn src = limitOrder();
        listener.onOrderIn(IoCodec.encodeOrderIn(src));

        verify(orderInPublisher).onData(any(DirectBuffer.class), anyInt(), anyInt(), eq(0));
    }

    @Test
    void nonSbePayloadRejectedAndNotDispatched() {
        OrderInPublisher orderInPublisher = mock(OrderInPublisher.class);
        MatchMetrics matchMetrics = mock(MatchMetrics.class);
        MessageConsumerListener listener = newListener(orderInPublisher, matchMetrics);

        // 长度足够的非 SBE 载荷：触发魔数检查拒绝（deserialization_failure）
        // 200 字节：超过最小帧长（visibleQty 扩展后 MIN≈108），命中魔数检查 → deserialization_failure
        listener.onOrderIn("x".repeat(200).getBytes(StandardCharsets.UTF_8));

        verify(orderInPublisher, never())
                .onData(any(DirectBuffer.class), anyInt(), anyInt(), anyInt());
        verify(matchMetrics).recordRejectedOrder("unknown", "deserialization_failure");
    }

    @Test
    void shortFrameRejectedAsFrameTooShort() {
        OrderInPublisher orderInPublisher = mock(OrderInPublisher.class);
        MatchMetrics matchMetrics = mock(MatchMetrics.class);
        MessageConsumerListener listener = newListener(orderInPublisher, matchMetrics);

        listener.onOrderIn("short".getBytes(StandardCharsets.UTF_8));

        verify(orderInPublisher, never())
                .onData(any(DirectBuffer.class), anyInt(), anyInt(), anyInt());
        verify(matchMetrics).recordRejectedOrder("unknown", "frame_too_short");
    }

    @Test
    void unknownSymbolRejectedAndNotDispatched() {
        OrderInPublisher orderInPublisher = mock(OrderInPublisher.class);
        MatchMetrics matchMetrics = mock(MatchMetrics.class);
        MessageConsumerListener listener = newListener(orderInPublisher, matchMetrics);

        OrderIn src =
                new OrderIn(
                        123L,
                        990001L,
                        "UNKNOWN",
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
        listener.onOrderIn(IoCodec.encodeOrderIn(src));

        verify(orderInPublisher, never())
                .onData(any(DirectBuffer.class), anyInt(), anyInt(), anyInt());
        verify(matchMetrics).recordRejectedOrder("unknown", "symbol_unknown");
    }

    private static MessageConsumerListener newListener(
            OrderInPublisher orderInPublisher, MatchMetrics matchMetrics) {
        MutableMatchEngineProperties props = new MutableMatchEngineProperties();
        props.setSymbols(Map.of("BTCUSDT", new MutableMatchEngineProperties.CoinScale()));
        MessageConsumerListener listener =
                new MessageConsumerListener(
                        orderInPublisher, matchMetrics, mock(DeadLetterSink.class), props, null);
        listener.initRoutes();
        return listener;
    }

    private static OrderIn limitOrder() {
        return new OrderIn(
                123L,
                990001L,
                "BTCUSDT",
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
    }
}
