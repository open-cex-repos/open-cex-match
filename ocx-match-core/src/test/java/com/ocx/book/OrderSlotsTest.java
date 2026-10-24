package com.ocx.book;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** 槽位纵表：freelist alloc/free、occupy/clear 占用语义、flags 位。 */
class OrderSlotsTest {

    @Test
    void freelistAllocFreeRoundTrip() {
        OrderSlots slots = new OrderSlots(4);
        assertEquals(0, slots.alloc());
        assertEquals(1, slots.alloc());
        assertEquals(2, slots.alloc());
        assertEquals(3, slots.alloc());
        assertEquals(-1, slots.alloc(), "槽位耗尽必须返回 -1");

        slots.occupy(1);
        slots.orderIds[1] = 42L;
        slots.clear(1);
        slots.free(1);
        assertEquals(1, slots.alloc(), "归还后的槽位必须可复用");
        assertEquals(-1, slots.next[1], "alloc 必须断开 freelist 残留链");
    }

    @Test
    void occupyClearTracksUsage() {
        OrderSlots slots = new OrderSlots(8);
        for (int i = 0; i < 8; i++) {
            assertFalse(slots.isOccupied(i));
            slots.occupy(i);
            slots.orderIds[i] = i + 1L;
            slots.setInBook(i, true);
        }
        assertEquals(8, slots.used());
        assertTrue(slots.isOccupied(3));

        slots.clear(3);
        assertEquals(7, slots.used());
        assertFalse(slots.isOccupied(3));
        assertEquals(0L, slots.orderIds[3]); // 空槽哨兵
        assertEquals(0, slots.flags[3]); // flags 已复位
        assertEquals(-1, slots.levels[3]); // 档位引用已清
    }

    @Test
    void flagBitsRoundTrip() {
        OrderSlots slots = new OrderSlots(4);
        int s = 1;
        slots.occupy(s);
        slots.setDirection(s, 0);
        assertTrue(slots.isBuy(s));
        slots.setDirection(s, 1);
        assertFalse(slots.isBuy(s));
        slots.setIceberg(s, true);
        assertTrue(slots.isIceberg(s));
        slots.setTif(s, 2);
        assertEquals(2, slots.tif(s));
        slots.setInBook(s, true);
        assertTrue(slots.isInBook(s));
        slots.setInBook(s, false);
        assertFalse(slots.isInBook(s));
    }

    @Test
    void remainingAndCompletion() {
        OrderSlots slots = new OrderSlots(4);
        int s = 2;
        slots.occupy(s);
        slots.origQtys[s] = 1_000_000;
        slots.execQtys[s] = 400_000;
        assertEquals(600_000, slots.remaining(s));
        assertFalse(slots.isCompleted(s));
        slots.execQtys[s] = 1_000_000;
        assertTrue(slots.isCompleted(s));
    }
}
