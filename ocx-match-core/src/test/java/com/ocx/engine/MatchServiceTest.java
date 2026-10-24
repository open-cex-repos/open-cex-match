package com.ocx.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.ocx.book.MatchFill;
import com.ocx.book.OrderBook;
import com.ocx.book.OrderRequest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/** 字段版撮合服务：FOK/POST_ONLY/IOC/市价单语义。 */
class MatchServiceTest {

    private static final int SCALE = 8;
    private OrderBook book;
    private final MatchService service = new MatchService(false);
    private final List<MatchFill> fills = new ArrayList<>();

    @BeforeEach
    void setUp() {
        book = new OrderBook("BTCUSDT", SCALE, SCALE, 1024, 256, 1_000_000L);
    }

    private static long p(double v) {
        return Math.round(v * Math.pow(10, SCALE));
    }

    private OrderRequest req(long orderId, int direction, double price, double qty, int tif) {
        OrderRequest r = new OrderRequest();
        r.orderId = orderId;
        r.userId = 10L;
        r.direction = direction;
        r.price = p(price);
        r.origQty = p(qty);
        r.timeInForce = tif;
        return r;
    }

    @Test
    void fokRejectedWhenInsufficientLiquidity() {
        book.addOrder(req(1, 0, 50000, 1.0, OrderRequest.TIF_GTC), 1000L);
        OrderRequest taker = req(2, 1, 50000, 5.0, OrderRequest.TIF_FOK);
        fills.clear();
        int rc = service.match(book, taker, 2000L, fills);
        assertEquals(MatchService.REJECT_FOK_INSUFFICIENT, rc);
        assertEquals(0, fills.size());
        assertEquals(1, book.openOrderCount()); // 簿不变
    }

    @Test
    void fokFillsWhenSufficient() {
        book.addOrder(req(1, 0, 50000, 2.0, OrderRequest.TIF_GTC), 1000L);
        OrderRequest taker = req(2, 1, 50000, 1.0, OrderRequest.TIF_FOK);
        fills.clear();
        assertEquals(MatchService.OK, service.match(book, taker, 2000L, fills));
        assertEquals(1, fills.size());
        assertEquals(1, book.openOrderCount()); // maker 剩 1.0
    }

    @Test
    void postOnlyRejectedWhenCrossing() {
        book.addOrder(req(1, 1, 50000, 1.0, OrderRequest.TIF_GTC), 1000L);
        OrderRequest taker = req(2, 0, 50000, 1.0, OrderRequest.TIF_POST_ONLY);
        fills.clear();
        int rc = service.match(book, taker, 2000L, fills);
        assertEquals(MatchService.REJECT_POST_ONLY_CROSS, rc);
        assertEquals(0, fills.size());
        assertEquals(1, book.openOrderCount());
    }

    @Test
    void postOnlyRestHangsWhenNotCrossing() {
        book.addOrder(req(1, 1, 50100, 1.0, OrderRequest.TIF_GTC), 1000L);
        OrderRequest taker = req(2, 0, 50000, 1.0, OrderRequest.TIF_POST_ONLY);
        fills.clear();
        assertEquals(MatchService.OK, service.match(book, taker, 2000L, fills));
        assertEquals(0, fills.size());
        assertEquals(2, book.openOrderCount()); // taker 已挂单
        assertEquals(2L, book.slots.orderIds[book.slotOf(2L)]); // taker 已入簿
    }

    @Test
    void iocDiscardsRestAfterPartialFill() {
        book.addOrder(req(1, 0, 50000, 1.0, OrderRequest.TIF_GTC), 1000L);
        OrderRequest taker = req(2, 1, 50000, 3.0, OrderRequest.TIF_IOC);
        fills.clear();
        assertEquals(MatchService.OK, service.match(book, taker, 2000L, fills));
        assertEquals(1, fills.size());
        assertEquals(0, book.openOrderCount()); // maker 吃穿，IOC 剩余丢弃
        assertFalse(book.slots.isOccupied(2)); // taker 未挂单，槽位空闲
    }

    @Test
    void marketEatsWholeBookAndDropsRest() {
        book.addOrder(req(1, 0, 50000, 1.0, OrderRequest.TIF_GTC), 1000L);
        book.addOrder(req(2, 0, 50010, 2.0, OrderRequest.TIF_GTC), 1001L);
        OrderRequest taker = new OrderRequest();
        taker.orderId = 9L;
        taker.userId = 20L;
        taker.direction = 1; // SELL market
        taker.orderType = OrderRequest.ORDER_TYPE_MARKET;
        taker.origQty = p(5.0);
        fills.clear();
        int rc = service.match(book, taker, 2000L, fills);
        assertEquals(MatchService.OK_MARKET_PARTIAL, rc); // 只吃穿 3.0，剩余 2.0 丢弃
        assertEquals(2, fills.size());
        assertEquals(0, book.openOrderCount());
        assertEquals(-1, book.bestPrice(0));
        assertEquals(2, fills.get(0).makerOrderId); // 买簿降序：先吃最高买价 50010（orderId 2）
        assertEquals(1, fills.get(1).makerOrderId); // 再吃 50000（orderId 1）
    }

    /**
     * 2026-08-30 成交对账回归：taker 部分成交后剩余挂单，再以 maker 成交时，
     * 累计成交额必须跨阶段单调（此前 addOrderInternal 把槽位 cumQuote 重置为 0，
     * 导致成交 recording 中同单 cumQuote 回落——下游资金核算会误判）。
     */
    @Test
    void partiallyFilledTakerRestThenMakerFillsCumQuoteMonotonic() {
        // 先挂卖单 100 @ 50000
        book.addOrder(req(1, 1, 50000, 100.0, OrderRequest.TIF_GTC), 1000L);
        // 买单 150 @ 50000：吃 100（taker 阶段），剩 50 挂单
        OrderRequest buy = req(2, 0, 50000, 150.0, OrderRequest.TIF_GTC);
        fills.clear();
        assertEquals(MatchService.OK, service.match(book, buy, 2000L, fills));
        assertEquals(1, fills.size());
        long takerQuote = fills.get(0).takerCumQuote;
        assertEquals(p(100.0 * 50000), takerQuote, "taker 阶段成交额 = 100×50000");
        assertEquals(1, book.openOrderCount(), "剩余 50 挂单");

        // 再挂卖单 30 @ 50000：吃掉剩余 50 中的 30（同一买单转 maker 阶段）
        OrderRequest sell = req(3, 1, 50000, 30.0, OrderRequest.TIF_GTC);
        fills.clear();
        assertEquals(MatchService.OK, service.match(book, sell, 3000L, fills));
        assertEquals(1, fills.size());
        MatchFill makerFill = fills.get(0);
        assertEquals(2L, makerFill.makerOrderId, "买单作 maker");
        assertEquals(
                takerQuote + p(30.0 * 50000),
                makerFill.makerCumQuote,
                "maker 阶段累计成交额 = taker 阶段 + 30×50000（不得回落）");
    }
}
