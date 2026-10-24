package com.ocx.recovery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ocx.codec.domain.ExchangeTrade;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * 成交记录对账校验器自检（2026-08-30）：合法成交 PASS；故意构造的违例样本
 * （turnover 不符 / 自成交 / 幽灵单 / 撤单后成交）必须 FAIL。
 */
class TradeRecordReconciliationTest {

    private static final int FUND_USERS = 8;

    private static ExchangeTrade trade(
            long buyId, long sellId, long priceM, long amtM, long buyToM, long sellToM, long time) {
        ExchangeTrade t = new ExchangeTrade();
        t.setPrice(priceM, 0);
        t.setAmount(amtM, 0);
        t.setBuyOrderId(buyId);
        t.setSellOrderId(sellId);
        t.setBuyTurnover(buyToM, 0);
        t.setSellTurnover(sellToM, 0);
        t.setTime(time);
        return t;
    }

    private static Map<Long, TradeRecordReconciliationMain.OrderInfo> orders(
            long buyId, long buyUser, long sellId, long sellUser) {
        Map<Long, TradeRecordReconciliationMain.OrderInfo> m = new HashMap<>();
        m.put(
                buyId,
                new TradeRecordReconciliationMain.OrderInfo(
                        buyId, buyUser, 100_000_000L, 0, (byte) 0, 1000L, false, 1L));
        m.put(
                sellId,
                new TradeRecordReconciliationMain.OrderInfo(
                        sellId, sellUser, 100_000_000L, 0, (byte) 1, 1000L, false, 2L));
        return m;
    }

    private static Map<Long, java.util.Set<TradeRecordReconciliationMain.OrderInfo>> liveByUser(
            Map<Long, TradeRecordReconciliationMain.OrderInfo> orders) {
        Map<Long, java.util.Set<TradeRecordReconciliationMain.OrderInfo>> m = new HashMap<>();
        for (TradeRecordReconciliationMain.OrderInfo o : orders.values()) {
            m.computeIfAbsent(o.userId, k -> new java.util.LinkedHashSet<>()).add(o);
        }
        return m;
    }

    @Test
    void 合法成交PASS并累计专用用户账本() {
        Map<Long, TradeRecordReconciliationMain.OrderInfo> orders = orders(11L, 1L, 22L, 2L);
        TradeRecordReconciliationMain.Result r = new TradeRecordReconciliationMain.Result();
        // price=100, amount=1_000_000 → 成交额 100_000_000；首笔 turnover == 成交额
        TradeRecordReconciliationMain.checkFill(
                trade(11L, 22L, 100L, 1_000_000L, 100_000_000L, 100_000_000L, 2000L),
                orders,
                liveByUser(orders),
                r,
                FUND_USERS);
        assertTrue(r.details.isEmpty(), "合法成交不应产生违例: " + r.details);
        assertEquals(1, r.fills);
        assertTrue(r.ledgers.containsKey(1L) && r.ledgers.containsKey(2L));
        assertEquals(
                0,
                TradeRecordReconciliationMain.dec(100_000_000L, 0)
                        .compareTo(r.ledgers.get(1L).buyQuote));
    }

    @Test
    void turnover增量不符FAIL() {
        Map<Long, TradeRecordReconciliationMain.OrderInfo> orders = orders(11L, 1L, 22L, 2L);
        TradeRecordReconciliationMain.Result r = new TradeRecordReconciliationMain.Result();
        // 首笔合法
        TradeRecordReconciliationMain.checkFill(
                trade(11L, 22L, 100L, 1_000_000L, 100_000_000L, 100_000_000L, 2000L),
                orders,
                liveByUser(orders),
                r,
                FUND_USERS);
        // 第二笔：turnover 增量应为 100_000_000（到 200_000_000），故意给 150_000_000
        TradeRecordReconciliationMain.checkFill(
                trade(11L, 22L, 100L, 1_000_000L, 150_000_000L, 150_000_000L, 3000L),
                orders,
                liveByUser(orders),
                r,
                FUND_USERS);
        assertTrue(r.u1Failures >= 2, "buy/sell 两侧增量不符都应抓到，实际 u1=" + r.u1Failures);
    }

    @Test
    void 自成交FAIL() {
        Map<Long, TradeRecordReconciliationMain.OrderInfo> orders = orders(11L, 1L, 22L, 1L);
        TradeRecordReconciliationMain.Result r = new TradeRecordReconciliationMain.Result();
        TradeRecordReconciliationMain.checkFill(
                trade(11L, 22L, 100L, 1_000_000L, 100_000_000L, 100_000_000L, 2000L),
                orders,
                liveByUser(orders),
                r,
                FUND_USERS);
        assertTrue(r.u4Failures == 1, "同 userId 自成交应 FAIL: " + r.details);
    }

    @Test
    void 幽灵单FAIL() {
        Map<Long, TradeRecordReconciliationMain.OrderInfo> orders = orders(11L, 1L, 22L, 2L);
        TradeRecordReconciliationMain.Result r = new TradeRecordReconciliationMain.Result();
        TradeRecordReconciliationMain.checkFill(
                trade(11L, 99L, 100L, 1_000_000L, 100_000_000L, 100_000_000L, 2000L),
                orders,
                liveByUser(orders),
                r,
                FUND_USERS);
        assertTrue(r.u5Failures >= 1, "引用未创建 orderId 应 FAIL: " + r.details);
    }

    @Test
    void 批撤后成交FAIL() {
        Map<Long, TradeRecordReconciliationMain.OrderInfo> orders = orders(11L, 1L, 22L, 2L);
        orders.get(22L).cancelled = true;
        orders.get(22L).cancelTime = 2500L;
        TradeRecordReconciliationMain.Result r = new TradeRecordReconciliationMain.Result();
        // fill 时间 3000 > 撤单时间 2500 → 已撤单仍成交
        TradeRecordReconciliationMain.checkFill(
                trade(11L, 22L, 100L, 1_000_000L, 100_000_000L, 100_000_000L, 3000L),
                orders,
                liveByUser(orders),
                r,
                FUND_USERS);
        assertTrue(r.u5Failures >= 1, "撤单后成交应 FAIL: " + r.details);
    }
}
