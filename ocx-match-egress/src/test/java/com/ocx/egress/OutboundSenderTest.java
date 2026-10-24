package com.ocx.egress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.ocx.book.MatchFill;
import com.ocx.codec.IoCodec;
import com.ocx.codec.domain.ExchangeTrade;
import com.ocx.outbound.OutboundDepth;

import io.aeron.Publication;

import org.agrona.DirectBuffer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** 纵表出站管线：MatchFill 值拷贝入队 → 字段级 SBE 批量编码 → Archive Publication offer。 */
class OutboundSenderTest {

    private static final int SCALE = 8;

    private static long p(double v) {
        return Math.round(v * Math.pow(10, SCALE));
    }

    @Test
    void fillsEncodedAndOfferedToArchiveTradeStream() {
        Publication pub = mock(Publication.class);
        when(pub.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(1024L);
        OutboundSender sender =
                new OutboundSender(
                        Map.of("BTCUSDT", pub),
                        Map.of("BTCUSDT", mock(Publication.class)),
                        null,
                        null);

        List<MatchFill> fills = new ArrayList<>();
        MatchFill f = new MatchFill();
        f.direction = 1; // SELL taker
        f.symbol = "BTCUSDT";
        f.takerOrderId = 2L;
        f.makerOrderId = 1L;
        f.price = p(50000);
        f.priceExponent = -SCALE;
        f.qty = p(1.0);
        f.qtyExponent = -SCALE;
        f.takerCumQuote = p(50000);
        f.makerCumQuote = p(50000);
        f.turnOverExponent = -SCALE;
        f.time = 1234L;
        fills.add(f);

        sender.submitFills(fills);
        assertEquals(1, sender.drainOnce());

        ArgumentCaptor<DirectBuffer> buf = ArgumentCaptor.forClass(DirectBuffer.class);
        ArgumentCaptor<Integer> len = ArgumentCaptor.forClass(Integer.class);
        verify(pub).offer(buf.capture(), eq(0), len.capture());

        byte[] payload = new byte[len.getValue()];
        buf.getValue().getBytes(0, payload);
        int frameLen =
                ((payload[0] & 0xFF) << 24)
                        | ((payload[1] & 0xFF) << 16)
                        | ((payload[2] & 0xFF) << 8)
                        | (payload[3] & 0xFF);
        ExchangeTrade decoded = IoCodec.decodeExchangeTrade(payload, IoCodec.FRAME_LENGTH_PREFIX);
        assertEquals("BTCUSDT", decoded.getSymbol());
        assertEquals(p(50000), decoded.getPriceMantissa());
        assertEquals(p(1.0), decoded.getAmountMantissa());
        assertEquals(2L, decoded.getSellOrderId()); // taker SELL
        assertEquals(1L, decoded.getBuyOrderId());
        assertEquals(p(50000), decoded.getSellTurnoverMantissa());
        assertEquals(1234L, decoded.getTime());
        assertEquals(frameLen + IoCodec.FRAME_LENGTH_PREFIX, payload.length);
    }

    @Test
    void depthEncodedAndOfferedToArchiveDepthStream() {
        Publication depthPub = mock(Publication.class);
        when(depthPub.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(64L);
        OutboundSender sender =
                new OutboundSender(
                        Map.of("BTCUSDT", mock(Publication.class)),
                        Map.of("BTCUSDT", depthPub),
                        null,
                        null);

        OutboundDepth depth = sender.acquireDepth("BTCUSDT", SCALE, SCALE);
        depth.bidPrices[0] = p(50000);
        depth.bidVolumes[0] = p(1.5);
        depth.bidCount = 1;
        depth.askPrices[0] = p(50100);
        depth.askVolumes[0] = p(2.0);
        depth.askCount = 1;
        depth.ts = 999L;
        sender.submitDepth(depth);
        assertEquals(1, sender.drainOnce());

        ArgumentCaptor<DirectBuffer> buf = ArgumentCaptor.forClass(DirectBuffer.class);
        ArgumentCaptor<Integer> len = ArgumentCaptor.forClass(Integer.class);
        verify(depthPub).offer(buf.capture(), eq(0), len.capture());
        assertTrue(len.getValue() > 0);
    }

    @Test
    void backpressureRetriesAndRecordsStats() {
        Publication pub = mock(Publication.class);
        when(pub.offer(any(DirectBuffer.class), anyInt(), anyInt()))
                .thenReturn(Publication.BACK_PRESSURED)
                .thenReturn(1024L);
        when(pub.isConnected()).thenReturn(true);
        OutboundStats stats = mock(OutboundStats.class);
        OutboundSender sender =
                new OutboundSender(Map.of("BTCUSDT", pub), Map.of("BTCUSDT", pub), null, stats);

        List<MatchFill> fills = new ArrayList<>();
        MatchFill f = new MatchFill();
        f.direction = 0; // BUY taker
        f.symbol = "BTCUSDT";
        f.takerOrderId = 1L;
        f.makerOrderId = 2L;
        f.price = p(50000);
        f.priceExponent = -SCALE;
        f.qty = p(1.0);
        f.qtyExponent = -SCALE;
        f.takerCumQuote = p(50000);
        f.makerCumQuote = p(50000);
        f.turnOverExponent = -SCALE;
        f.time = 1L;
        fills.add(f);

        sender.submitFills(fills);
        assertEquals(1, sender.drainOnce());
        verify(stats).recordBackpressure("BTCUSDT");
        verify(stats).recordPublished(eq("BTCUSDT"), anyLong());
    }
}
