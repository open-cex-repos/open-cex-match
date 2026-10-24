package com.ocx.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.ocx.book.MatchFill;
import com.ocx.book.OrderBook;
import com.ocx.book.TriggerSlots;
import com.ocx.codec.domain.OrderInFields;
import com.ocx.metrics.MatchMetrics;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;

/** 纵表引擎装配链路：OrderInFields → 校验 → 转换 → 撮合 → 输出回调。 */
class MatchEngineTest {

    private static final int SCALE = 8;
    private OrderBook book;
    private TriggerSlots triggers;
    private MatchEngine engine;
    private final List<MatchFill> outFills = new ArrayList<>();
    private final List<String> rejects = new ArrayList<>();
    private long lastBatchCancelUser = Long.MIN_VALUE;

    @BeforeEach
    void setUp() {
        book = new OrderBook("BTCUSDT", SCALE, SCALE, 1024, 256, 1_000_000L);
        triggers = new TriggerSlots(1024, 256);
        engine =
                new MatchEngine(
                        book,
                        triggers,
                        new MatchService(false),
                        outFills::addAll,
                        (orderId, userId, reason) -> rejects.add(reason),
                        null);
    }

    private OrderInFields in(
            long id, long userId, int side, int type, int tif, double price, double qty) {
        OrderInFields f = new OrderInFields();
        f.id = id;
        f.userId = userId;
        f.side = (byte) side;
        f.type = (byte) type;
        f.timeInForce = (byte) tif;
        f.priceMantissa = Math.round(price * Math.pow(10, SCALE));
        f.priceExponent = -SCALE;
        f.origQtyMantissa = Math.round(qty * Math.pow(10, SCALE));
        f.origQtyExponent = -SCALE;
        return f;
    }

    @Test
    void limitOrderFillsAndEmits() {
        assertEquals(0, engine.onOrderIn(in(1, 10, 0, 1, 0, 50000, 1.0), 1000L));
        assertEquals(0, engine.onOrderIn(in(2, 20, 1, 1, 0, 50000, 1.0), 2000L));
        assertEquals(1, outFills.size());
        assertEquals(2L, outFills.get(0).takerOrderId);
        assertEquals(1L, outFills.get(0).makerOrderId);
        assertTrue(rejects.isEmpty());
        assertEquals(0, book.openOrderCount());
    }

    @Test
    void invalidOrderRejected() {
        OrderInFields bad = in(0, 10, 0, 1, 0, 50000, 1.0);
        assertEquals(1, engine.onOrderIn(bad, 1000L));
        assertEquals(List.of("order_id_null"), rejects);
        assertEquals(0, book.openOrderCount());
    }

    @Test
    void marketOrderConsumesWholeSide() {
        engine.onOrderIn(in(1, 10, 0, 1, 0, 50000, 1.0), 1000L);
        OrderInFields market = in(2, 20, 1, 2, 0, 0, 3.0);
        assertEquals(0, engine.onOrderIn(market, 2000L));
        assertEquals(1, outFills.size());
        assertEquals(0, book.openOrderCount());
        assertTrue(rejects.isEmpty());
    }

    @Test
    void batchCancelRoutesByUserId() {
        engine.onOrderIn(in(1, 10, 0, 1, 0, 50000, 1.0), 1000L);
        engine.onOrderIn(in(2, 10, 1, 1, 0, 50001, 1.0), 1001L);
        OrderInFields cancel = in(0, 10, 0, 1, 0, 0, 0);
        cancel.status = 99;
        assertEquals(2, engine.onOrderIn(cancel, 2000L));
        assertEquals(0, book.openOrderCount());
    }

    @Test
    void conditionalOrderMounted() {
        OrderInFields stop = in(1, 10, 0, 3, 0, 50000, 1.0);
        stop.mergeLowPriceMantissa = 51000;
        stop.mergeLowPriceExponent = -SCALE;
        assertEquals(6, engine.onOrderIn(stop, 1000L)); // 条件单已挂载
        assertEquals(0, book.openOrderCount());
        assertEquals(0, book.slotsUsed()); // 条件单不在限价簿（TriggerSlots 管）
        assertEquals(1, triggers.orderCount());
        assertTrue(rejects.isEmpty());
    }

    @Test
    void triggerConvertsToLimitOrder() {
        // STOP_LIMIT 买单：触发价 50100，限价 50200，触发后转限价入簿
        OrderInFields stop = in(1, 10, 0, 3, 0, 50200, 1.0);
        stop.mergeLowPriceMantissa = 50100;
        stop.mergeLowPriceExponent = -SCALE;
        assertEquals(6, engine.onOrderIn(stop, 1000L));

        // 卖单挂 50300（不与转单价 50200 交叉）：盘口价 50300 触发条件单（last >= 50100）
        engine.onOrderIn(in(2, 20, 1, 1, 0, 50300, 1.0), 2000L);
        assertEquals(0, triggers.orderCount()); // 已触发离簿
        assertEquals(2, book.openOrderCount()); // 卖单 50300 + 转限价买 50200
        assertEquals(1L, book.slots.orderIds[book.slotOf(1L)]); // 转限价单已入簿
        assertEquals(2L, book.slots.orderIds[book.slotOf(2L)]); // 卖单保留
    }

    @Test
    void duplicateOrderIdRejectedBeforeMatching() {
        // 同号买单已挂簿；重复单换成反方向且价格可成交 → 必须整单拒绝（duplicate），
        // 不得再撮合产生同号二次成交（2026-09-01 判重提前到撮合前）
        assertEquals(0, engine.onOrderIn(in(1, 10, 0, 1, 0, 50000, 1.0), 1000L));
        assertEquals(1, engine.onOrderIn(in(1, 20, 1, 1, 0, 50000, 1.0), 2000L));
        assertEquals(List.of("duplicate_order_id"), rejects);
        assertEquals(0, outFills.size());
        assertEquals(1, book.openOrderCount()); // 簿不变
    }

    @Test
    void unmatchedOrderWithFullBookRejected() {
        // 小 cap 满簿：完全不成交（价格不交叉）的 GTC 单挂单被 cap 拒绝
        OrderBook small = new OrderBook("BTCUSDT", SCALE, SCALE, 16, 8, 2L);
        MatchEngine smallEngine =
                new MatchEngine(
                        small,
                        new TriggerSlots(16, 8),
                        new MatchService(false),
                        outFills::addAll,
                        (orderId, userId, reason) -> rejects.add(reason),
                        null);
        assertEquals(0, smallEngine.onOrderIn(in(1, 10, 0, 1, 0, 50000, 1.0), 1000L));
        assertEquals(0, smallEngine.onOrderIn(in(2, 10, 0, 1, 0, 50000, 1.0), 1001L));
        // 卖 1.0 @50100：高于买一 50000 不成交，簿满（cap=2）挂单被拒
        assertEquals(1, smallEngine.onOrderIn(in(3, 20, 1, 1, 0, 50100, 1.0), 2000L));
        assertEquals(0, outFills.size()); // 无成交
        assertEquals(List.of("order_book_cap"), rejects);
        assertEquals(2, small.openOrderCount()); // 原有 2 单保留
    }

    @Test
    void matchLatencyRecordedWithRealElapsed() {
        MatchMetrics metrics = mock(MatchMetrics.class);
        when(metrics.mode()).thenReturn(MatchMetrics.Mode.FULL);
        MatchEngine metricEngine =
                new MatchEngine(
                        book,
                        triggers,
                        new MatchService(false),
                        outFills::addAll,
                        (orderId, userId, reason) -> {
                            rejects.add(reason);
                            metrics.recordRejectedOrder(
                                    "BTCUSDT", reason); // 模拟 OrderEventHandler 的 RejectSink
                        },
                        metrics);
        metricEngine.onOrderIn(in(1, 10, 0, 1, 0, 50000, 1.0), 1000L);
        metricEngine.onOrderIn(in(2, 20, 1, 1, 0, 50000, 1.0), 2000L);

        ArgumentCaptor<Long> elapsed = ArgumentCaptor.forClass(Long.class);
        verify(metrics).recordMatch(eq("BTCUSDT"), eq("limit"), eq(1), elapsed.capture());
        assertTrue(elapsed.getValue() >= 0L);
    }

    @Test
    void noFillOrderStillRecordsLatency() {
        MatchMetrics metrics = mock(MatchMetrics.class);
        when(metrics.mode()).thenReturn(MatchMetrics.Mode.FULL);
        MatchEngine metricEngine =
                new MatchEngine(
                        book,
                        triggers,
                        new MatchService(false),
                        outFills::addAll,
                        (orderId, userId, reason) -> rejects.add(reason),
                        metrics);
        metricEngine.onOrderIn(in(1, 10, 0, 1, 0, 50000, 1.0), 1000L);
        metricEngine.onOrderIn(in(2, 20, 1, 1, 0, 50100, 1.0), 2000L); // 不交叉 → 挂单

        // 两笔都无成交（挂买单 + 不交叉卖单），各记录一次延迟
        verify(metrics, times(2)).recordMatch(eq("BTCUSDT"), eq("limit"), eq(0), anyLong());
    }

    @Test
    void fokPrecheckRejectSkipsLatency() {
        MatchMetrics metrics = mock(MatchMetrics.class);
        when(metrics.mode()).thenReturn(MatchMetrics.Mode.FULL);
        MatchEngine metricEngine =
                new MatchEngine(
                        book,
                        triggers,
                        new MatchService(false),
                        outFills::addAll,
                        (orderId, userId, reason) -> {
                            rejects.add(reason);
                            metrics.recordRejectedOrder(
                                    "BTCUSDT", reason); // 模拟 OrderEventHandler 的 RejectSink
                        },
                        metrics);
        metricEngine.onOrderIn(in(1, 10, 0, 1, 0, 50000, 1.0), 1000L);
        metricEngine.onOrderIn(in(2, 20, 1, 1, 2, 50000, 5.0), 2000L); // FOK 流动性不足

        // 只有第一笔挂买单记录延迟；FOK 预检失败不记录
        verify(metrics, times(1)).recordMatch(anyString(), anyString(), anyInt(), anyLong());
        verify(metrics).recordRejectedOrder(eq("BTCUSDT"), eq("fok_insufficient_liquidity"));
    }
}
