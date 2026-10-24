package com.ocx.book;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

/** 价格档纵表：排序遍历（BUY 降序 / SELL 升序）、getOrCreate、删除、FIFO、随机对拍。 */
class LevelSlotsTest {

    @Test
    void sellLevelsAscending() {
        LevelSlots levels = new LevelSlots(false, 64, 8);
        long[] prices = {50000, 50100, 49900, 50050};
        for (long p : prices) {
            levels.getOrCreate(p);
        }
        List<Long> ordered = collectPrices(levels);
        assertEquals(List.of(49900L, 50000L, 50050L, 50100L), ordered);
    }

    @Test
    void buyLevelsDescending() {
        LevelSlots levels = new LevelSlots(true, 64, 8);
        long[] prices = {50000, 50100, 49900, 50050};
        for (long p : prices) {
            levels.getOrCreate(p);
        }
        List<Long> ordered = collectPrices(levels);
        assertEquals(List.of(50100L, 50050L, 50000L, 49900L), ordered);
    }

    @Test
    void getOrCreateIsIdempotent() {
        LevelSlots levels = new LevelSlots(false, 64, 8);
        int a = levels.getOrCreate(12345L);
        int b = levels.getOrCreate(12345L);
        assertEquals(a, b);
        assertEquals(1, levels.levelCount());
        assertEquals(12345L, levels.price(a));
    }

    @Test
    void levelCacheHitAndInvalidatedOnRemove() {
        LevelSlots levels = new LevelSlots(false, 64, 8);
        int a = levels.getOrCreate(100L);
        assertEquals(a, levels.getOrCreate(100L)); // 命中缓存
        assertEquals(a, levels.get(100L)); // 命中缓存
        levels.removeLevel(a);
        assertEquals(-1, levels.get(100L)); // removeLevel 后缓存失效
        int b = levels.getOrCreate(100L); // 重新建档（缓存重新填充）
        assertEquals(100L, levels.price(b));
    }

    @Test
    void removeLevelUnlinks() {
        LevelSlots levels = new LevelSlots(true, 64, 8);
        levels.getOrCreate(100L);
        int mid = levels.getOrCreate(90L);
        levels.getOrCreate(80L);
        assertEquals(3, levels.levelCount());
        levels.removeLevel(mid);
        assertEquals(2, levels.levelCount());
        assertEquals(-1, levels.get(90L));
        assertEquals(List.of(100L, 80L), collectPrices(levels));
    }

    @Test
    void fifoAppendUnlinkMoveToTail() {
        LevelSlots levels = new LevelSlots(false, 16, 8);
        OrderSlots slots = new OrderSlots(16);
        int level = levels.getOrCreate(50000L);

        int s1 = 1; // 测试直接占用槽位（簿内 freelist 分配由 OrderBook 负责）
        int s2 = 2;
        int s3 = 3;
        slots.occupy(s1);
        slots.occupy(s2);
        slots.occupy(s3);
        slots.orderIds[s1] = s1;
        slots.orderIds[s2] = s2;
        slots.orderIds[s3] = s3;
        levels.append(level, s1, slots);
        levels.append(level, s2, slots);
        levels.append(level, s3, slots);
        assertEquals(3, levels.count(level));
        assertEquals(s1, levels.heads[level]);
        assertEquals(s3, levels.tails[level]);
        assertEquals(List.of(s1, s2, s3), chain(slots, levels.heads[level]));

        // 摘中间
        levels.unlink(level, s2, slots);
        assertEquals(List.of(s1, s3), chain(slots, levels.heads[level]));

        // 冰山重排：s1 移队尾
        levels.moveToTail(level, s1, slots);
        assertEquals(List.of(s3, s1), chain(slots, levels.heads[level]));
        assertEquals(s1, levels.tails[level]);
        assertEquals(2, levels.count(level));
    }

    @Test
    void depthVolumeAggregation() {
        LevelSlots levels = new LevelSlots(false, 16, 8);
        int level = levels.getOrCreate(50000L);
        levels.normalRemaining[level] = 5_000_000;
        levels.icebergDisplayed[level] = 2_000_000;
        assertEquals(7_000_000, levels.depthVolume(level));
    }

    @Test
    void randomOpsAgainstTreeMap() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        for (int round = 0; round < 10; round++) {
            // 容量必须远大于随机序列的活跃档位数峰值（70% 插入/30% 删除的随机游走）
            LevelSlots levels = new LevelSlots(false, 4096, 12);
            TreeMap<Long, Integer> ref = new TreeMap<>();
            for (int i = 0; i < 2_000; i++) {
                long price = 10_000 + rnd.nextInt(100_000);
                int op = rnd.nextInt(10);
                if (op < 7) {
                    int ls = levels.getOrCreate(price);
                    ref.put(price, ls);
                } else {
                    int ls = levels.get(price);
                    if (ls != -1) {
                        levels.removeLevel(ls);
                        ref.remove(price);
                    }
                }
            }
            assertEquals(ref.size(), levels.levelCount());
            long expect = ref.isEmpty() ? -1 : ref.firstKey();
            int first = levels.firstLevel();
            assertEquals(expect, first == -1 ? -1 : levels.price(first));

            // 全序遍历与 TreeMap 一致
            List<Long> actual = collectPrices(levels);
            List<Long> expected = new ArrayList<>(ref.keySet());
            assertEquals(expected, actual);
            for (long p : ref.keySet()) {
                assertEquals((long) ref.get(p), levels.get(p));
            }
        }
    }

    private static List<Integer> chain(OrderSlots slots, int head) {
        List<Integer> result = new ArrayList<>();
        int cur = head;
        while (cur != -1) {
            result.add(cur);
            cur = slots.next[cur];
        }
        return result;
    }

    private static List<Long> collectPrices(LevelSlots levels) {
        List<Long> result = new ArrayList<>();
        int ls = levels.firstLevel();
        while (ls != -1) {
            result.add(levels.price(ls));
            ls = levels.nextLevel(ls);
        }
        return result;
    }
}
