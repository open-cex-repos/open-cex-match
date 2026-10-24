package com.ocx.book;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.ocx.codec.domain.OrderInFields;
import com.ocx.engine.MatchEngine;
import com.ocx.engine.MatchService;

import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 进程内复现测试：直接驱动引擎灌单（固定价随机双边，无成交），
 * 检查满簿拒绝是否在 openOrderCount 远低于 cap 时过早出现（疑似槽位/档位泄漏）。
 * 2026-09-01 起 orderId 不再内嵌槽位（orderId→slot 映射 + freelist），
 * 不存在槽位回绕冲突，cap 拒绝只可能发生在簿真正满时。
 */
class BookCapStressReproTest {

    private static final int SCALE = 8;
    private static final long BASE = 70_000L * pow10(SCALE);

    @Test
    void capRejectShouldNotFireBeforeBookIsFull() {
        int cap = 5_000_000;
        int orderCapacity = cap;
        int levelCapacity = Math.max(cap / 16, 1024);
        OrderBook book = new OrderBook("BTCUSDT", SCALE, SCALE, orderCapacity, levelCapacity, cap);
        AtomicLong fills = new AtomicLong();
        AtomicLong capRejects = new AtomicLong();
        AtomicLong otherRejects = new AtomicLong();
        AtomicLong firstCapAt = new AtomicLong(-1);
        AtomicLong firstCapOpen = new AtomicLong(-1);
        AtomicLong firstCapUsed = new AtomicLong(-1);

        MatchEngine engine =
                new MatchEngine(
                        book,
                        new TriggerSlots(4096, 64),
                        new MatchService(false),
                        fills1 -> fills.addAndGet(fills1.size()),
                        (orderId, userId, reason) -> {
                            if ("order_book_cap".equals(reason)) {
                                long n = capRejects.incrementAndGet();
                                if (n == 1) {
                                    firstCapAt.set(n);
                                    firstCapOpen.set(book.openOrderCount());
                                    firstCapUsed.set(book.slotsUsed());
                                }
                            } else {
                                otherRejects.incrementAndGet();
                            }
                        },
                        null);

        Random rng = new Random(42);
        double mid = BASE;
        long orderId = 1L;
        long start = System.nanoTime();
        for (int batch = 0; batch < 40_000 && orderId < 15_000_000L; batch++) { // 至多 1500 万单
            for (int i = 0; i < 500; i++) {
                if (capRejects.get() > 0 || orderId >= 15_000_000L) {
                    break;
                }
                int side = rng.nextBoolean() ? 0 : 1;
                double d = -10.0 * Math.log(1.0 - rng.nextDouble());
                if (d > 200) {
                    d = 200;
                }
                double price = side == 0 ? mid - d : mid + d;
                OrderInFields f = new OrderInFields();
                f.id = orderId++;
                f.userId = 1L;
                f.priceMantissa = Math.round(price);
                f.priceExponent = -SCALE;
                f.origQtyMantissa = 150_000_000L;
                f.origQtyExponent = -SCALE;
                f.executedQtyMantissa = 0L;
                f.executedQtyExponent = -SCALE;
                f.side = (byte) side;
                f.type = 1;
                f.timeInForce = 0;
                f.createTimeMillis = 0L;
                engine.onOrderIn(f, System.currentTimeMillis());
                // 注入重复单：每 1000 单把刚发的随机订单原样再发一次（若上一单挂住则触发
                // 撮合前判重路径；重复单不得触碰槽位/freelist）
                if ((orderId - 1) % 1000 == 0) {
                    engine.onOrderIn(f, System.currentTimeMillis());
                }
            }
            if (capRejects.get() > 0) {
                break;
            }
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000L;

        System.out.println("灌单 " + (orderId - 1) + " 单，耗时 " + elapsedMs + "ms");
        System.out.println(
                "成交=" + fills.get() + " cap拒绝=" + capRejects.get() + " 其它拒绝=" + otherRejects.get());
        System.out.println(
                "openOrderCount="
                        + book.openOrderCount()
                        + " slotsUsed="
                        + book.slotsUsed()
                        + " cap="
                        + cap);
        if (capRejects.get() > 0) {
            System.out.println(
                    "首个 cap 拒绝时: open=" + firstCapOpen.get() + " used=" + firstCapUsed.get());
        }

        // 关键断言：满簿拒绝不应发生在 openOrderCount 远低于 cap 时
        if (capRejects.get() > 0) {
            assertEquals(
                    cap,
                    firstCapOpen.get(),
                    "cap 拒绝过早触发：openOrderCount="
                            + firstCapOpen.get()
                            + " used="
                            + firstCapUsed.get());
        }
    }

    private static long pow10(int n) {
        long v = 1;
        for (int i = 0; i < n; i++) {
            v *= 10;
        }
        return v;
    }
}
