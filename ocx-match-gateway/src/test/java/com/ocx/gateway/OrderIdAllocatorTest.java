package com.ocx.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

class OrderIdAllocatorTest {

    @TempDir Path tempDir;

    private static final long SEG_SHIFT = 42L;

    @Test
    void allocateProducesSnowflakeOrderIdsWithStablePrefix() {
        OrderIdAllocator allocator = new OrderIdAllocator(100L, null);
        long seg = allocator.allocate() >> SEG_SHIFT;
        long prev = allocator.currentCounter();
        for (int i = 0; i < 1000; i++) {
            long id = allocator.allocate();
            assertEquals(seg, id >> SEG_SHIFT, "同发号方时间段必须恒定（段内）");
            assertEquals(100L, (id >>> OrderIdAllocator.COUNTER_BITS) & 0xFF, "issuer 位必须为发号方 ID");
            assertTrue(id > 0, "id 必须为正");
            assertTrue(allocator.currentCounter() > prev, "counter 必须递增");
            prev = allocator.currentCounter();
        }
    }

    @Test
    void layoutDecodesToSegIssuerCounter() {
        // 纯业务号布局：seg/issuer/counter 三段可解码，低位与引擎槽位无关
        OrderIdAllocator allocator = new OrderIdAllocator(7L, null);
        long prev = 0L;
        for (int i = 0; i < 3000; i++) {
            long id = allocator.allocate();
            long seg = id >>> 42;
            long issuer = (id >>> OrderIdAllocator.COUNTER_BITS) & 0xFF;
            long counter = id & ((1L << OrderIdAllocator.COUNTER_BITS) - 1);
            assertEquals(allocator.segment(), seg, "高位必须为当前小时段");
            assertEquals(7L, issuer, "issuer 位必须为发号方 ID");
            assertTrue(counter > prev, "counter 必须段内递增");
            prev = counter;
        }
    }

    @Test
    void watermarkPersistsAcrossRestartWithoutReusingIds() {
        Path state = tempDir.resolve("counter.watermark");
        OrderIdAllocator first = new OrderIdAllocator(100L, state);
        Set<Long> firstBatch = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            firstBatch.add(first.allocate());
        }

        // 重启：新 allocator 从持久化 (seg, counter) 恢复，同段续发不撞历史
        OrderIdAllocator second = new OrderIdAllocator(100L, state);
        assertEquals(first.segment(), second.segment(), "重启后必须留在同一时间段");
        assertTrue(second.currentCounter() >= 100, "水位应从文件恢复");
        for (int i = 0; i < 100; i++) {
            long id = second.allocate();
            assertTrue(!firstBatch.contains(id), "重启后新 orderId 不得与历史单重复（同段水位续发）");
        }
    }

    @Test
    void segmentAdvancesWithClockAndCounterResets() {
        AtomicLong clock = new AtomicLong(1_800_000_000_000L); // 2027-01-17 附近
        Path state = tempDir.resolve("seg.watermark");
        OrderIdAllocator allocator = new OrderIdAllocator(100L, state, clock::get);
        long segA = allocator.segment();
        Set<Long> batchA = new HashSet<>();
        for (int i = 0; i < 50; i++) {
            batchA.add(allocator.allocate());
        }
        long counterA = allocator.currentCounter();

        // 跨过小时边界：新段 counter 重发，但高位段不同 → 不撞历史
        clock.addAndGet(OrderIdAllocator.SEG_MS);
        long firstNew = allocator.allocate();
        assertEquals(segA + 1, allocator.segment(), "段必须随小时边界推进");
        assertTrue(allocator.currentCounter() < counterA, "新段 counter 必须重发（远小于旧段水位）");
        assertEquals(segA + 1, firstNew >> SEG_SHIFT, "新单必须落在新段");
        assertTrue(!batchA.contains(firstNew), "跨段单号不得与历史重复");
    }

    @Test
    void clockRollbackKeepsSegmentAndContinuesCounter() {
        AtomicLong clock = new AtomicLong(1_800_000_000_000L);
        Path state = tempDir.resolve("rollback.watermark");
        OrderIdAllocator allocator = new OrderIdAllocator(100L, state, clock::get);
        long seg = allocator.segment();
        for (int i = 0; i < 100; i++) {
            allocator.allocate();
        }
        long counter = allocator.currentCounter();

        // 模拟时钟回拨（回退 3 小时）后重启：段不降、同段 counter 续发
        clock.addAndGet(-3L * OrderIdAllocator.SEG_MS);
        OrderIdAllocator restarted = new OrderIdAllocator(100L, state, clock::get);
        assertEquals(seg, restarted.segment(), "时钟回拨时 seg 不得回退");
        long id = restarted.allocate();
        assertTrue(id >> SEG_SHIFT == seg, "回拨后仍在原段");
        assertTrue(restarted.currentCounter() > counter, "同段 counter 必须续发");
    }

    @Test
    void counterExhaustionAdvancesSegmentEarly() {
        long seg = 500_000L;
        Path state = tempDir.resolve("exhaust.watermark");
        try {
            java.nio.file.Files.writeString(
                    state, seg + " " + (OrderIdAllocator.COUNTER_MAX - 1) + "\n");
        } catch (java.io.IOException e) {
            throw new AssertionError(e);
        }
        AtomicLong clock = new AtomicLong(seg * OrderIdAllocator.SEG_MS + 1L);
        OrderIdAllocator allocator = new OrderIdAllocator(100L, state, clock::get);
        long id1 = allocator.allocate(); // 填满段内最后一号
        long id2 = allocator.allocate(); // 段内耗尽 → 提前推进一段
        assertEquals(seg, id1 >> SEG_SHIFT);
        assertEquals(seg + 1, id2 >> SEG_SHIFT, "counter 耗尽必须提前推进时间段");
        assertEquals(1L, allocator.currentCounter(), "新段 counter 从 1 重发");
    }

    @Test
    void segmentExhaustionThrowsInsteadOfNegativeId() {
        long seg = (1L << OrderIdAllocator.SEG_BITS) - 1; // 段空间顶格
        Path state = tempDir.resolve("seg-exhaust.watermark");
        try {
            java.nio.file.Files.writeString(
                    state, seg + " " + (OrderIdAllocator.COUNTER_MAX - 1) + "\n");
        } catch (java.io.IOException e) {
            throw new AssertionError(e);
        }
        AtomicLong clock = new AtomicLong(seg * OrderIdAllocator.SEG_MS + 1L);
        OrderIdAllocator allocator = new OrderIdAllocator(100L, state, clock::get);
        allocator.allocate(); // 填满顶格段内最后一号
        try {
            allocator.allocate(); // 段内耗尽 → 推进越过段空间 → 必须拒绝
            throw new AssertionError("seg 耗尽应拒绝发号");
        } catch (IllegalStateException expected) {
            // expected
        }
    }

    @Test
    void issuerIdBoundsEnforced() {
        try {
            new OrderIdAllocator(0L, null);
            throw new AssertionError("issuerId=0 应被拒绝");
        } catch (IllegalArgumentException expected) {
            // expected
        }
        try {
            new OrderIdAllocator(OrderIdAllocator.ISSUER_MAX + 1, null);
            throw new AssertionError("issuerId 越界应被拒绝");
        } catch (IllegalArgumentException expected) {
            // expected
        }
    }
}
