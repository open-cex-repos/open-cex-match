package com.ocx.book;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/** 纵表订单簿核心语义：入簿/撮合/冰山/撤单/批撤/聚合。价格 coinScale=8、数量 baseCoinScale=8 定点。 */
class OrderBookTest {

    private static final int SCALE = 8;
    private OrderBook book;
    private final List<MatchFill> fills = new ArrayList<>();

    @BeforeEach
    void setUp() {
        book = new OrderBook("BTCUSDT", SCALE, SCALE, 1024, 256, 1_000_000L);
    }

    private static long p(double v) {
        return Math.round(v * Math.pow(10, SCALE));
    }

    private OrderRequest req(long orderId, long userId, int direction, double price, double qty) {
        OrderRequest r = new OrderRequest();
        r.orderId = orderId;
        r.userId = userId;
        r.direction = direction;
        r.price = p(price);
        r.origQty = p(qty);
        r.timeInForce = OrderRequest.TIF_GTC;
        return r;
    }

    @Test
    void limitOrderAddsToBookAndAggregates() {
        assertEquals(0, book.addOrder(req(1, 10, 0, 50000, 2.0), 1000L));
        assertEquals(0, book.addOrder(req(2, 10, 0, 50000, 1.5), 1001L));
        assertEquals(1, book.priceLevelSize(0));
        assertEquals(p(3.5), book.buyLevels.normalRemaining[book.buyLevels.firstLevel()]);
        assertEquals(2, book.openOrderCount());
        assertEquals(p(50000), book.bestPrice(0));
    }

    @Test
    void buySellOpenCountsTrackBook() {
        // 2 笔买单 + 1 笔卖单
        assertEquals(0, book.addOrder(req(1, 10, 0, 50000, 2.0), 1000L));
        assertEquals(0, book.addOrder(req(2, 10, 0, 49900, 1.5), 1001L));
        assertEquals(0, book.addOrder(req(3, 20, 1, 50100, 1.0), 1002L));
        assertEquals(2, book.buyOpenCount());
        assertEquals(1, book.sellOpenCount());
        assertEquals(book.openOrderCount(), book.buyOpenCount() + book.sellOpenCount());

        // 撤单 1 笔买单（direction 计数同步减少）
        assertTrue(book.cancel(2L));
        assertEquals(1, book.buyOpenCount());
        assertEquals(1, book.sellOpenCount());
        assertEquals(book.openOrderCount(), book.buyOpenCount() + book.sellOpenCount());

        // 卖单全量穿越买一（matchTaker 移除 maker，买单计数归零）
        fills.clear();
        OrderRequest taker = req(4, 30, 1, 50000, 2.0);
        assertEquals(0L, book.matchTaker(taker, 2000L, false, fills));
        assertEquals(0, book.buyOpenCount());
        assertEquals(1, book.sellOpenCount());
        assertEquals(1, fills.size());
        assertEquals(book.openOrderCount(), book.buyOpenCount() + book.sellOpenCount());
    }

    @Test
    void crossTakerMatchesMakerAndClearsBook() {
        book.addOrder(req(1, 10, 0, 50000, 1.0), 1000L);
        OrderRequest taker = req(2, 20, 1, 50000, 1.0);
        long remaining = book.matchTaker(taker, 2000L, false, fills);
        assertEquals(0L, remaining);
        assertEquals(1, fills.size());
        assertEquals(2L, fills.get(0).takerOrderId);
        assertEquals(1L, fills.get(0).makerOrderId);
        assertEquals(p(50000), fills.get(0).price);
        assertEquals(p(1.0), fills.get(0).qty);
        assertEquals(0, book.openOrderCount());
        assertEquals(-1, book.bestPrice(0));
        assertEquals(-1, book.bestPrice(1));
        assertEquals(0, book.slotsUsed()); // maker 槽位已归还
        assertEquals(-1, book.orderIndex.get(1L)); // 映射已摘除
        assertEquals(0, book.buyLevels.levelCount());
    }

    @Test
    void partialFillRestGtcOrder() {
        book.addOrder(req(1, 10, 0, 50000, 2.0), 1000L);
        OrderRequest taker = req(2, 20, 1, 50000, 0.5);
        long remaining = book.matchTaker(taker, 2000L, false, fills);
        assertEquals(0L, remaining); // 全部成交，taker 无剩余
        assertEquals(1, fills.size());
        // taker 剩余 0，无挂单；maker 部分成交留在簿
        assertEquals(1, book.openOrderCount());
        assertEquals(p(1.5), book.buyLevels.normalRemaining[book.buyLevels.firstLevel()]);
    }

    @Test
    void takerPartiallyFilledThenRestHangs() {
        book.addOrder(req(1, 10, 1, 50000, 1.0), 1000L);
        OrderRequest taker = req(2, 20, 0, 50000, 3.0);
        long remaining = book.matchTaker(taker, 2000L, false, fills);
        assertEquals(p(2.0), remaining); // 吃到 1.0，剩余 2.0 挂单
        assertEquals(1, fills.size());
        // matchTaker 只返回剩余量（挂单由 MatchService 处理）
        assertEquals(0, book.openOrderCount());
        assertEquals(0, book.slotsUsed()); // taker 未挂单，槽位空闲
        assertEquals(-1, book.orderIndex.get(2L));
        // 显式挂单（service 语义）
        taker.origQty = remaining;
        assertEquals(0, book.addOrder(taker, 2000L));
        assertEquals(1, book.openOrderCount());
        assertEquals(p(2.0), book.buyLevels.normalRemaining[book.buyLevels.firstLevel()]);
        assertEquals(2L, book.slots.orderIds[book.orderIndex.get(2L)]); // taker 已入簿
    }

    @Test
    void icebergDisplayOnlyAggregatedAndHiddenCanBeEaten() {
        // 冰山买单：orig=2.0 visible=0.5 → 盘口只显示 0.5
        OrderRequest ice = req(1, 10, 0, 50000, 2.0);
        ice.visibleQty = p(0.5);
        assertEquals(0, book.addOrder(ice, 1000L));
        int level = book.buyLevels.firstLevel();
        assertEquals(p(0.5), book.buyLevels.icebergDisplayed[level]);
        assertEquals(0L, book.buyLevels.normalRemaining[level]);
        assertEquals(p(0.5), book.buyLevels.depthVolume(level)); // 隐藏量不显示

        // taker 吃 0.5（吃穿显示量但吃不完隐藏）→ 刷新显示量回队尾
        OrderRequest taker1 = req(2, 20, 1, 50000, 0.5);
        assertEquals(0L, book.matchTaker(taker1, 2000L, false, fills));
        assertEquals(1, book.openOrderCount()); // 冰山仍在簿
        int slot = book.orderIndex.get(1L); // orderId→slot 映射
        assertEquals(p(0.5), book.slots.displayedQtys[slot]); // 已刷新
        assertEquals(p(0.5), book.buyLevels.icebergDisplayed[book.buyLevels.firstLevel()]);

        // 再来一笔 taker 吃 1.5 → 吃穿刷新后的显示量 + 隐藏量，冰山完成
        OrderRequest taker2 = req(3, 30, 1, 50000, 1.5);
        fills.clear();
        assertEquals(0L, book.matchTaker(taker2, 3000L, false, fills));
        assertEquals(1, fills.size()); // taker2 一笔吃穿剩余 1.5
        assertEquals(0, book.openOrderCount());
        assertEquals(-1, book.bestPrice(0));
    }

    @Test
    void cancelRemovesOrderAndAggregation() {
        book.addOrder(req(1, 10, 0, 50000, 1.0), 1000L);
        book.addOrder(req(2, 10, 0, 50000, 2.0), 1001L);
        assertTrue(book.cancel(1L));
        assertEquals(1, book.openOrderCount());
        assertEquals(p(2.0), book.buyLevels.normalRemaining[book.buyLevels.firstLevel()]);
        assertEquals(-1, book.orderIndex.get(1L)); // 撤单后映射摘除
        assertFalse(book.cancel(1L)); // 幂等
    }

    @Test
    void batchCancelRemovesUserOrdersByScan() {
        book.addOrder(req(1, 10, 0, 50000, 1.0), 1000L);
        book.addOrder(req(2, 10, 1, 50001, 1.0), 1001L);
        book.addOrder(req(3, 20, 0, 50000, 1.0), 1002L);
        // 单撤 1 号后全簿扫描批撤：已撤订单不在簿，自然跳过
        book.cancel(1L);
        int count = book.batchCancel(10L);
        assertEquals(1, count); // 只撤 2 号（1 号已撤）
        assertEquals(1, book.openOrderCount()); // 3 号保留
        assertEquals(3L, book.slots.orderIds[book.orderIndex.get(3L)]);
        assertEquals(-1, book.orderIndex.get(2L));
    }

    @Test
    void capAndDuplicateRejected() {
        OrderBook small = new OrderBook("S", 8, 8, 4, 4, 2L);
        assertEquals(0, small.addOrder(req(1, 1, 0, 50000, 1.0), 0L));
        assertEquals(0, small.addOrder(req(2, 1, 0, 50001, 1.0), 0L));
        assertEquals(1, small.addOrder(req(3, 1, 0, 50002, 1.0), 0L)); // cap
        assertEquals(1, small.addOrder(req(1, 1, 0, 50003, 1.0), 0L)); // 满簿时 cap 优先于判重
        assertEquals(1, small.addOrder(req(4, 1, 1, 50004, 1.0), 0L)); // cap 满，卖单同样拒绝

        // 未满时重复 orderId 才走到判重拒绝
        OrderBook fresh = new OrderBook("S", 8, 8, 4, 4, 100L);
        assertEquals(0, fresh.addOrder(req(1, 1, 0, 50000, 1.0), 0L));
        assertEquals(2, fresh.addOrder(req(1, 1, 0, 50001, 1.0), 0L)); // 重复

        // 不启用 cap 时，槽位耗尽返回 cap 拒绝语义
        OrderBook slotBound = new OrderBook("S", 8, 8, 4, 4, 0L);
        for (long i = 1; i <= 4; i++) {
            assertEquals(0, slotBound.addOrder(req(i, 1, 0, 50000 + i, 1.0), 0L));
        }
        assertEquals(1, slotBound.addOrder(req(5, 1, 0, 50005, 1.0), 0L)); // 槽位耗尽
    }

    @Test
    void orderIdsAreIndependentOfSlots() {
        // 2026-09-01 起 orderId 不再内嵌槽位：不同 orderId 可同时在簿（各占一个
        // freelist 槽位），不再有"同槽不同代际"冲突拒绝；判重只看 orderId 本身。
        OrderBook book = new OrderBook("S", 8, 8, 4, 4, 100L);
        assertEquals(0, book.addOrder(req(1, 1, 0, 50000, 1.0), 0L));
        assertEquals(0, book.addOrder(req(5, 1, 0, 50001, 1.0), 0L)); // 不同 id 共存
        assertTrue(book.orderIndex.get(1L) != book.orderIndex.get(5L), "不同订单必须落在不同槽位");
        assertEquals(2, book.addOrder(req(1, 1, 0, 50002, 1.0), 0L)); // 同 id 判重
        assertTrue(book.cancel(5L));
        assertFalse(book.cancel(5L)); // 幂等未命中
        assertTrue(book.cancel(1L));
        assertEquals(0, book.slotsUsed());
    }

    @Test
    void multiLevelDepthOrdering() {
        book.addOrder(req(1, 10, 0, 50100, 1.0), 1000L);
        book.addOrder(req(2, 10, 0, 50000, 2.0), 1001L);
        book.addOrder(req(3, 10, 0, 50050, 1.5), 1002L);
        long[] prices = new long[8];
        long[] vols = new long[8];
        int n = book.copyLevels(0, prices, vols, 8);
        assertEquals(3, n);
        assertEquals(p(50100), prices[0]);
        assertEquals(p(50050), prices[1]);
        assertEquals(p(50000), prices[2]);
        assertEquals(p(1.0), vols[0]);
        assertEquals(p(1.5), vols[1]);
        assertEquals(p(2.0), vols[2]);
    }

    @Test
    void selfTradePreventionSkipsMaker() {
        book.addOrder(req(1, 10, 0, 50000, 1.0), 1000L);
        OrderRequest taker = req(2, 10, 1, 50000, 1.0);
        long remaining = book.matchTaker(taker, 2000L, true, fills);
        assertEquals(p(1.0), remaining); // 同 user 跳过，不成交
        assertEquals(0, fills.size());
        assertEquals(1, book.openOrderCount()); // maker 保留；剩余挂单由 MatchService 处理
    }
}
