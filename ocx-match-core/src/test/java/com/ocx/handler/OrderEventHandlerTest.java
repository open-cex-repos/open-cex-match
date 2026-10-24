package com.ocx.handler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.ocx.book.OrderBook;
import com.ocx.book.TriggerSlots;
import com.ocx.codec.domain.OrderIn;
import com.ocx.engine.MatchService;
import com.ocx.math.Decimal;
import com.ocx.outbound.FillSink;
import com.ocx.outbound.OutboundDepth;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** 纵表事件处理器：SBE 帧 → MatchEngine → 出站（成交/深度），零 Order 对象。 */
class OrderEventHandlerTest {

    private static final int SCALE = 8;

    private OrderBook book;
    private FillSink outbound;
    private OrderEventHandler handler;

    @BeforeEach
    void setUp() {
        book = new OrderBook("BTCUSDT", SCALE, SCALE, 1024, 256, 1_000_000L);
        outbound = mock(FillSink.class);
        when(outbound.acquireDepth(anyString(), anyInt(), anyInt()))
                .thenReturn(new OutboundDepth());
        handler =
                new OrderEventHandler(
                        book,
                        new TriggerSlots(1024, 256),
                        new MatchService(false),
                        outbound,
                        null,
                        null);
    }

    private static OrderIn in(long id, long userId, int side, int type, double price, double qty) {
        return new OrderIn(
                id,
                userId,
                "BTCUSDT",
                price <= 0 ? null : Decimal.of(Math.round(price * Math.pow(10, SCALE)), -SCALE),
                Decimal.of(Math.round(qty * Math.pow(10, SCALE)), -SCALE),
                Decimal.of(0L, -SCALE),
                null,
                (byte) side,
                (byte) type,
                "GTC",
                null,
                null,
                null);
    }

    @Test
    void limitCrossFillsAndPublishesDepth() {
        // 挂买单 50000
        handler.onEvent(SbeTestSupport.event(in(1, 10, 0, 1, 50000, 1.0)), 0L, false);
        // 卖单 50000 成交（endOfBatch：深度 + WAL flush）
        handler.onEvent(SbeTestSupport.event(in(2, 20, 1, 1, 50000, 1.0)), 1L, true);

        verify(outbound).submitFills(anyList());
        verify(outbound).submitDepth(any());
        org.junit.jupiter.api.Assertions.assertEquals(0, book.openOrderCount());
    }

    @Test
    void noCrossNoFillsDepthStillPublished() {
        handler.onEvent(SbeTestSupport.event(in(1, 10, 0, 1, 50000, 1.0)), 0L, true);
        verify(outbound, never()).submitFills(anyList()); // 无成交
        verify(outbound).submitDepth(any());
    }

    @Test
    void multipleFillBatchesReusePoolContainers() {
        // 第一批：挂单 + 成交（批末 flush 后容器归还池）
        handler.onEvent(SbeTestSupport.event(in(1, 10, 0, 1, 50000, 1.0)), 0L, false);
        handler.onEvent(SbeTestSupport.event(in(2, 20, 1, 1, 50000, 1.0)), 1L, true);
        verify(outbound).submitFills(anyList());

        // 第二批：再从池取容器（回归：ReusableFillList.remove(int) 必须可弹出）
        handler.onEvent(SbeTestSupport.event(in(3, 30, 0, 1, 51000, 1.0)), 2L, false);
        handler.onEvent(SbeTestSupport.event(in(4, 40, 1, 1, 51000, 1.0)), 3L, true);
        verify(outbound, times(2)).submitFills(anyList());
    }

    @Test
    void emptyEventSkipsMatch() {
        OrderEvent empty = new OrderEvent();
        empty.setSbeLength(0);
        empty.setSnapshotRequest(false);
        handler.onEvent(empty, 0L, true);
        verify(outbound, never()).submitFills(anyList());
        verify(outbound).submitDepth(any()); // flush 仍发布深度
    }
}
