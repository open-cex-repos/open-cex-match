package com.ocx.book;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** 条件单槽位纵表：挂载/触发/撤单/trailing 语义。触发价 coinScale=8 定点。 */
class TriggerSlotsTest {

    private static final int SCALE = 8;

    private static long p(double v) {
        return Math.round(v * Math.pow(10, SCALE));
    }

    @Test
    void mountCancelOrder() {
        TriggerSlots triggers = new TriggerSlots(64, 16);
        assertEquals(
                0,
                triggers.add(
                        1L,
                        10L,
                        p(50100),
                        p(50200),
                        p(1.0),
                        0,
                        TriggerSlots.TYPE_STOP_LIMIT,
                        0L,
                        1000L));
        assertEquals(1, triggers.orderCount());
        assertFalse(triggers.isEmpty());
        assertTrue(triggers.cancel(1L));
        assertEquals(0, triggers.orderCount());
        assertFalse(triggers.cancel(1L)); // 幂等
    }

    @Test
    void triggerPriceLevelTableFullReturnsCapNotThrow() {
        // 触发价档位容量只有 4：塞满后新触发价应返回 1（容量满），而不是抛异常
        TriggerSlots triggers = new TriggerSlots(64, 4);
        assertEquals(
                0,
                triggers.add(
                        1L,
                        10L,
                        p(50100),
                        p(50200),
                        p(1.0),
                        0,
                        TriggerSlots.TYPE_STOP_LIMIT,
                        0L,
                        1000L));
        assertEquals(
                0,
                triggers.add(
                        2L,
                        10L,
                        p(50200),
                        p(50300),
                        p(1.0),
                        0,
                        TriggerSlots.TYPE_STOP_LIMIT,
                        0L,
                        1000L));
        assertEquals(
                0,
                triggers.add(
                        3L,
                        10L,
                        p(50300),
                        p(50400),
                        p(1.0),
                        0,
                        TriggerSlots.TYPE_STOP_LIMIT,
                        0L,
                        1000L));
        assertEquals(
                0,
                triggers.add(
                        4L,
                        10L,
                        p(50400),
                        p(50500),
                        p(1.0),
                        0,
                        TriggerSlots.TYPE_STOP_LIMIT,
                        0L,
                        1000L));
        // 第 5 个不同触发价 → 档位表满 → 容量满（1），不抛异常
        assertEquals(
                1,
                triggers.add(
                        5L,
                        10L,
                        p(50500),
                        p(50600),
                        p(1.0),
                        0,
                        TriggerSlots.TYPE_STOP_LIMIT,
                        0L,
                        1000L));
        // 已挂载的条件单不受影响，可正常触发
        int[] out = new int[16];
        assertEquals(1, triggers.checkAndTrigger(p(50150), out));
    }

    @Test
    void triggerLimitBuyAboveTriggerPrice() {
        TriggerSlots triggers = new TriggerSlots(64, 16);
        triggers.add(
                1L, 10L, p(50100), p(50200), p(1.0), 0, TriggerSlots.TYPE_STOP_LIMIT, 0L, 1000L);
        int[] out = new int[16];
        // lastPrice 50000：未到触发价
        assertEquals(0, triggers.checkAndTrigger(p(50000), out));
        // lastPrice 50150：跨过 50100 触发
        assertEquals(1, triggers.checkAndTrigger(p(50150), out));
        int slot = out[0];
        assertEquals(1L, triggers.orderIds[slot]);
        assertEquals(p(50200), triggers.prices[slot]); // 转限价价格
        triggers.release(slot);
        assertEquals(0, triggers.orderCount());
    }

    @Test
    void triggerSellBelowTriggerPrice() {
        TriggerSlots triggers = new TriggerSlots(64, 16);
        triggers.add(
                1L, 10L, p(49900), p(49800), p(1.0), 1, TriggerSlots.TYPE_STOP_MARKET, 0L, 1000L);
        int[] out = new int[16];
        assertEquals(0, triggers.checkAndTrigger(p(50000), out)); // 未到
        assertEquals(1, triggers.checkAndTrigger(p(49850), out)); // 跌破 49900
        assertEquals(0, triggers.orderCount());
    }

    @Test
    void takeProfitSellTriggersAbove() {
        TriggerSlots triggers = new TriggerSlots(64, 16);
        triggers.add(
                1L,
                10L,
                p(51000),
                p(50900),
                p(1.0),
                1,
                TriggerSlots.TYPE_TAKE_PROFIT_MARKET,
                0L,
                1000L);
        int[] out = new int[16];
        assertEquals(0, triggers.checkAndTrigger(p(50500), out));
        assertEquals(1, triggers.checkAndTrigger(p(51100), out)); // 涨到 51100 ≥ 51000
    }

    @Test
    void samePriceLevelTriggersAllInOrder() {
        TriggerSlots triggers = new TriggerSlots(64, 16);
        triggers.add(
                1L, 10L, p(50100), p(50200), p(1.0), 0, TriggerSlots.TYPE_STOP_LIMIT, 0L, 1000L);
        triggers.add(
                2L, 10L, p(50100), p(50200), p(1.0), 0, TriggerSlots.TYPE_STOP_LIMIT, 0L, 1001L);
        int[] out = new int[16];
        assertEquals(2, triggers.checkAndTrigger(p(50150), out));
        assertEquals(1L, triggers.orderIds[out[0]]); // FIFO：先挂先触发
        assertEquals(2L, triggers.orderIds[out[1]]);
        triggers.release(out[0]);
        triggers.release(out[1]);
        assertEquals(0, triggers.orderCount());
    }

    @Test
    void trailingStopUpdatesAndTriggers() {
        TriggerSlots triggers = new TriggerSlots(64, 16);
        triggers.add(
                1L,
                10L,
                p(50000),
                p(50000),
                p(1.0),
                0,
                TriggerSlots.TYPE_TRAILING_STOP,
                p(100),
                1000L);
        // 买 trailing：trigger = max(50000, bestBid - 100)
        triggers.updateTrailingStops(p(50100), p(50150));
        int[] out = new int[16];
        // lastPrice 49950：低于触发价 50000，不触发
        assertEquals(0, triggers.checkAndTrigger(p(49950), out));
        // 触发价已更新到 50000（bestBid 50100 - 100）；last 50000 触发
        assertEquals(1, triggers.checkAndTrigger(p(50000), out));
        assertTrue(out[0] != -1);
    }

    /** 触发扫描 O(1) 预检：删掉最高触发档后（maxKey 过期），下一轮低档仍必须触发。 */
    @Test
    void triggerAfterMaxKeyLevelRemoved() {
        TriggerSlots triggers = new TriggerSlots(64, 16);
        // 同一 BUY stop 表挂两档：50100（高价）与 49900（低价）
        triggers.add(
                1L, 10L, p(50100), p(50200), p(1.0), 0, TriggerSlots.TYPE_STOP_LIMIT, 0L, 1000L);
        triggers.add(
                2L, 10L, p(49900), p(50000), p(1.0), 0, TriggerSlots.TYPE_STOP_LIMIT, 0L, 1000L);
        int[] out = new int[16];

        // 价格 50200：跨过 50100（且跨过 49900）→ 两档都应触发
        assertEquals(2, triggers.checkAndTrigger(p(50200), out));
        triggers.release(out[0]);
        triggers.release(out[1]);
        assertEquals(0, triggers.orderCount());

        // 重新挂单：先挂 50100 并触发（删除最高档，maxKey 过期），再验证 49900 仍能触发
        triggers.add(
                3L, 10L, p(50100), p(50200), p(1.0), 0, TriggerSlots.TYPE_STOP_LIMIT, 0L, 1000L);
        triggers.add(
                4L, 10L, p(49900), p(50000), p(1.0), 0, TriggerSlots.TYPE_STOP_LIMIT, 0L, 1000L);
        // 触发价 50050：只跨过 49900（50100 > 50050 不触发）
        assertEquals(1, triggers.checkAndTrigger(p(50050), out));
        assertEquals(4L, triggers.orderIds[out[0]]);
        triggers.release(out[0]);
        assertEquals(1, triggers.orderCount());

        // 再涨到 50200：剩余 50100 档触发（预检状态已恢复，maxKey=50100 档）
        assertEquals(1, triggers.checkAndTrigger(p(50200), out));
        assertEquals(3L, triggers.orderIds[out[0]]);
        triggers.release(out[0]);
        assertEquals(0, triggers.orderCount());
    }

    @Test
    void forEachEnumeratesAllMountedStops() {
        TriggerSlots triggers = new TriggerSlots(256, 16);
        triggers.add(
                1L, 10L, p(50100), p(50200), p(1.0), 0, TriggerSlots.TYPE_STOP_LIMIT, 0L, 1000L);
        triggers.add(2L, 20L, p(49900), 0L, p(2.0), 1, TriggerSlots.TYPE_STOP_MARKET, 0L, 1001L);
        triggers.add(
                3L,
                30L,
                p(50500),
                p(50600),
                p(0.5),
                1,
                TriggerSlots.TYPE_TAKE_PROFIT_LIMIT,
                0L,
                1002L);
        triggers.add(
                4L, 40L, p(51000), 0L, p(3.0), 0, TriggerSlots.TYPE_TAKE_PROFIT_MARKET, 0L, 1003L);
        triggers.add(
                5L, 50L, p(50200), 0L, p(1.5), 1, TriggerSlots.TYPE_TRAILING_STOP, p(100), 1004L);
        // 撤一个：遍历应只剩 4（含 trailing 链上的 5）
        assertTrue(triggers.cancel(3L));

        java.util.List<com.ocx.snapshot.TriggerSnapshotFields> all = new java.util.ArrayList<>();
        triggers.forEach(all::add);
        assertEquals(4, all.size(), "遍历应覆盖 8 表 + trailing 链的全部在挂条件单");
        java.util.Map<Long, com.ocx.snapshot.TriggerSnapshotFields> byId =
                new java.util.HashMap<>();
        for (com.ocx.snapshot.TriggerSnapshotFields f : all) {
            byId.put(f.orderId(), f);
        }
        assertFalse(byId.containsKey(3L), "已撤条件单不应出现在遍历中");

        com.ocx.snapshot.TriggerSnapshotFields f1 = byId.get(1L);
        assertEquals(p(50100), f1.triggerPrice());
        assertEquals(p(50200), f1.price());
        assertEquals(p(1.0), f1.origQty());
        assertEquals(0, f1.direction());
        assertEquals(TriggerSlots.TYPE_STOP_LIMIT, f1.triggerType());
        assertEquals(0L, f1.trailDelta());

        com.ocx.snapshot.TriggerSnapshotFields f5 = byId.get(5L);
        assertEquals(TriggerSlots.TYPE_TRAILING_STOP, f5.triggerType());
        assertEquals(p(100), f5.trailDelta());
        assertEquals(1, f5.direction());
    }
}
