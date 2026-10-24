package com.ocx.book;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/** long→int 开地址哈希：基本操作、覆盖、删除 rehash、随机对拍。 */
class Long2IntMapTest {

    /** 墓碑化回归：每轮插入一批后全量删除（live 有界、触发墓碑复用与就地重建），对拍 HashMap。 */
    @Test
    void tombstoneChurnMatchesHashMap() {
        int rounds = 6;
        int perRound = 5000;
        Long2IntMap map = new Long2IntMap(perRound);
        Map<Long, Integer> ref = new HashMap<>();
        long seq = 1L;
        for (int r = 0; r < rounds; r++) {
            // 插入一批
            for (int i = 0; i < perRound; i++) {
                map.put(seq, (int) seq);
                ref.put(seq, (int) seq);
                seq++;
            }
            // 全量删除本轮键（live 保持有界，不超构造容量；墓碑大量累积触发重建）
            for (long k = seq - perRound; k < seq; k++) {
                assertEquals((int) k, map.remove(k));
                ref.remove(k);
            }
            // 全部已删键不可查
            for (long k = seq - perRound; k < seq; k++) {
                assertEquals(-1, map.get(k));
            }
            assertEquals(ref.size(), map.size());
        }
        // 覆盖/重复键语义
        map.put(1L, 999);
        assertEquals(999, map.get(1L));
        assertEquals(999, map.put(1L, 1000));
        assertEquals(1000, map.remove(1L));
        assertEquals(-1, map.remove(1L));
    }

    @Test
    void basicPutGetRemove() {
        Long2IntMap map = new Long2IntMap(16);
        assertEquals(-1, map.get(42L));
        assertEquals(-1, map.put(42L, 7));
        assertEquals(7, map.get(42L));
        assertEquals(1, map.size());
        assertEquals(7, map.put(42L, 9));
        assertEquals(9, map.get(42L));
        assertEquals(1, map.size());
        assertEquals(9, map.remove(42L));
        assertEquals(-1, map.get(42L));
        assertEquals(0, map.size());
    }

    @Test
    void zeroKeySupported() {
        Long2IntMap map = new Long2IntMap(8);
        assertEquals(-1, map.put(0L, 3));
        assertEquals(3, map.get(0L));
        assertTrue(map.containsKey(0L));
        assertEquals(3, map.remove(0L));
        assertFalse(map.containsKey(0L));
    }

    @Test
    void collisionChainSurvivesDeletion() {
        // 容量 64 → 实际 128 槽；64 个 key 负载 0.5，删除后 rehash 有足够空槽（表满会死循环）
        Long2IntMap map = new Long2IntMap(64);
        for (long k = 0; k < 64; k++) {
            map.put(k, (int) k);
        }
        // 删除前半，再验证后半全部可查（删除 rehash 必须保持探测链正确）
        for (long k = 0; k < 32; k++) {
            assertEquals((int) k, map.remove(k));
        }
        for (long k = 32; k < 64; k++) {
            assertEquals((int) k, map.get(k));
        }
        assertEquals(32, map.size());
    }

    @Test
    void putOverCapacityThrowsInsteadOfHanging() {
        Long2IntMap map = new Long2IntMap(16);
        int capacity = map.capacity();
        for (int i = 0; i < capacity; i++) {
            map.put(i, i);
        }
        assertThrows(IllegalStateException.class, () -> map.put(capacity, capacity));
    }

    @Test
    void randomOpsAgainstHashMap() {
        // 容量须覆盖 10 万次随机操作的活跃 key 峰值（key 池 2^20，期望活跃 ~5 万）
        Long2IntMap map = new Long2IntMap(65_536);
        Map<Long, Integer> ref = new HashMap<>();
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        for (int i = 0; i < 100_000; i++) {
            long key = rnd.nextLong() & 0xFFFFF;
            int op = rnd.nextInt(10);
            if (op < 5) {
                int v = rnd.nextInt(1_000_000);
                map.put(key, v);
                ref.put(key, v);
            } else if (op < 8) {
                assertEquals(ref.getOrDefault(key, -1), map.get(key));
            } else {
                Integer expect = ref.remove(key);
                assertEquals(expect == null ? -1 : expect, map.remove(key));
            }
        }
        for (Map.Entry<Long, Integer> e : ref.entrySet()) {
            assertEquals((int) e.getValue(), map.get(e.getKey()));
        }
        assertEquals(ref.size(), map.size());
    }
}
