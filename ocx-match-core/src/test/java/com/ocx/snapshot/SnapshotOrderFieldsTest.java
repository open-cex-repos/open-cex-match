package com.ocx.snapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ocx.codec.sbe.wal.MessageHeaderEncoder;
import com.ocx.codec.sbe.wal.OrderCreateEncoder;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/** 快照订单字段版编解码往返（OrderRecordCodec OCX1 SBE，SnapshotCodec 复用）。 */
class SnapshotOrderFieldsTest {

    private static final int SCALE = 8;

    @Test
    void encodeDecodeFieldsRoundTrip() {
        byte[] payload = new byte[OrderRecordCodec.MAGIC_LEN + OrderRecordCodec.MAX_ENCODED_LENGTH];
        UnsafeBuffer buffer = new UnsafeBuffer(payload);
        int len =
                OrderRecordCodec.encodeFields(
                        "BTCUSDT",
                        12345L,
                        678L,
                        Math.round(50000 * Math.pow(10, SCALE)),
                        Math.round(1.5 * Math.pow(10, SCALE)),
                        0,
                        2,
                        123456789L,
                        Math.round(0.3 * Math.pow(10, SCALE)),
                        Math.round(21000 * Math.pow(10, SCALE)),
                        buffer,
                        0,
                        new byte[512],
                        new MessageHeaderEncoder(),
                        new OrderCreateEncoder());
        assertEquals(true, len > 0);

        byte[] exact = new byte[len];
        System.arraycopy(payload, 0, exact, 0, len);
        SnapshotOrderFields f = OrderRecordCodec.decodeFields(exact);
        assertEquals(12345L, f.orderId);
        assertEquals(678L, f.userId);
        assertEquals("BTCUSDT", f.symbol);
        assertEquals(Math.round(50000 * Math.pow(10, SCALE)), f.price);
        assertEquals(Math.round(1.5 * Math.pow(10, SCALE)), f.origQty);
        assertEquals(0, f.direction);
        assertEquals(2, f.tif);
        assertEquals(123456789L, f.time);
        // 2026-09-01 补充：快照预填 U1 基线依赖 cumQuoteQty/executedQty，旧格式
        // 固定块与 var 长度槽重叠会静默错读，必须断言
        assertEquals(Math.round(0.3 * Math.pow(10, SCALE)), f.executedQty);
        assertEquals(Math.round(21000 * Math.pow(10, SCALE)), f.cumQuoteQty);
    }

    @Test
    void roundTripSurvivesDirtyScratchAndReuse() {
        // 2026-09-01 回归：旧 putString 跳过缺失 var 槽位，复用 scratch 时残留
        // 垃圾被解码成"长度"→ 快照预填整体失败（对账 U5 尾部根因）。全零 scratch
        // 会掩盖该问题，这里用脏 scratch 强制暴露。
        byte[] payload = new byte[OrderRecordCodec.MAGIC_LEN + OrderRecordCodec.MAX_ENCODED_LENGTH];
        UnsafeBuffer buffer = new UnsafeBuffer(payload);
        byte[] dirty = new byte[512];
        java.util.Arrays.fill(dirty, (byte) 0x7F);
        int len =
                OrderRecordCodec.encodeFields(
                        "BTCUSDT",
                        999L,
                        888L,
                        Math.round(60000 * Math.pow(10, SCALE)),
                        Math.round(2.0 * Math.pow(10, SCALE)),
                        1,
                        0,
                        55L,
                        0L,
                        0L,
                        buffer,
                        0,
                        dirty,
                        new MessageHeaderEncoder(),
                        new OrderCreateEncoder());
        assertTrue(len > 0);
        SnapshotOrderFields f =
                OrderRecordCodec.decodeFields(java.util.Arrays.copyOf(payload, len));
        assertEquals(999L, f.orderId);
        assertEquals(888L, f.userId);
        assertEquals("BTCUSDT", f.symbol);
        assertEquals(0, f.tif);
        assertEquals(1, f.direction);
        assertEquals(0L, f.executedQty);
        assertEquals(0L, f.cumQuoteQty);
    }
}
