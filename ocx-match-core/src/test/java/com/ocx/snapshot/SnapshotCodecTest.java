package com.ocx.snapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ocx.codec.sbe.wal.MessageHeaderEncoder;
import com.ocx.codec.sbe.wal.OrderCreateEncoder;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * 快照 recording 编解码回环（2026-08-30，Aeron Archive recording 快照）：META/END
 * 固定帧 + ORDER/STOP 批量帧（OCX1 负载）编码 → 解码逐字段一致；批量帧超限 flush。
 * 完整性判定（END 缺失/计数不符弃用）在恢复端（WalArchiveRecoveryTest 覆盖回退路径）。
 */
class SnapshotCodecTest {

    private static final int SCALE = 8;
    private static final String SYMBOL = "BTCUSDT";

    private static long p(double v) {
        return Math.round(v * Math.pow(10, SCALE));
    }

    private static SnapshotOrderFields order(
            long id, long userId, long price, long qty, int direction, int tif, long time) {
        SnapshotOrderFields f = new SnapshotOrderFields();
        f.orderId = id;
        f.userId = userId;
        f.symbol = SYMBOL;
        f.price = price;
        f.priceExponent = 0;
        f.origQty = qty;
        f.origQtyExponent = 0;
        f.direction = direction;
        f.tif = tif;
        f.time = time;
        return f;
    }

    @Test
    void metaRoundTrip() {
        SnapshotCodec.SnapshotMeta src =
                new SnapshotCodec.SnapshotMeta(SYMBOL, 1787310000000L, 8, 8, 3, 5, 77L, 123456789L);
        UnsafeBuffer buf = new UnsafeBuffer(new byte[SnapshotCodec.META_LEN]);
        int len = SnapshotCodec.encodeMeta(buf, 0, src);

        assertEquals(SnapshotCodec.META_LEN, len, "META 定长");
        assertTrue(SnapshotCodec.hasMagic(buf, 0));
        assertEquals(SnapshotCodec.TYPE_META, SnapshotCodec.type(buf, 0));
        assertEquals(src, SnapshotCodec.decodeMeta(SYMBOL, buf, 0), "META 逐字段一致");
    }

    @Test
    void endRoundTrip() {
        UnsafeBuffer buf = new UnsafeBuffer(new byte[SnapshotCodec.END_LEN]);
        int len = SnapshotCodec.encodeEnd(buf, 0, 3, 5);

        assertEquals(SnapshotCodec.END_LEN, len, "END 定长");
        assertEquals(SnapshotCodec.TYPE_END, SnapshotCodec.type(buf, 0));
        int[] counts = SnapshotCodec.decodeEndCounts(buf, 0);
        assertEquals(3, counts[0], "END orderCount");
        assertEquals(5, counts[1], "END triggerCount");
    }

    @Test
    void orderBatchRoundTrip() {
        List<SnapshotOrderFields> orders =
                List.of(
                        order(101L, 7L, p(70000), p(1.0), 0, 0, 1787310000000L),
                        order(202L, 8L, p(70001), p(0.5), 1, 3, 1787310001000L),
                        order(303L, 7L, p(69999), p(0.25), 0, 1, 1787310002000L));

        UnsafeBuffer batch = new UnsafeBuffer(new byte[SnapshotCodec.MAX_BATCH_BYTES]);
        UnsafeBuffer rec =
                new UnsafeBuffer(
                        new byte[OrderRecordCodec.MAX_ENCODED_LENGTH + OrderRecordCodec.MAGIC_LEN]);
        byte[] strScratch =
                new byte[OrderRecordCodec.MAX_ENCODED_LENGTH + OrderRecordCodec.MAGIC_LEN];
        MessageHeaderEncoder header = new MessageHeaderEncoder();
        OrderCreateEncoder enc = new OrderCreateEncoder();
        int len = SnapshotCodec.encodeBatchHeader(batch, 0, SnapshotCodec.TYPE_ORDER_BATCH, 0);
        int count = 0;
        for (SnapshotOrderFields o : orders) {
            int recLen =
                    OrderRecordCodec.encodeFields(
                            SYMBOL,
                            o.orderId,
                            o.userId,
                            o.price,
                            o.origQty,
                            o.direction,
                            o.tif,
                            o.time,
                            0L,
                            0L,
                            rec,
                            0,
                            strScratch,
                            header,
                            enc);
            len = SnapshotCodec.appendRecord(batch, 0, len, rec, 0, recLen);
            count++;
        }
        SnapshotCodec.encodeBatchHeader(batch, 0, SnapshotCodec.TYPE_ORDER_BATCH, count);

        assertEquals(SnapshotCodec.TYPE_ORDER_BATCH, SnapshotCodec.type(batch, 0));
        assertEquals(3, SnapshotCodec.batchCount(batch, 0), "批量帧记录数");
        List<SnapshotOrderFields> decoded = new ArrayList<>();
        SnapshotCodec.forEachRecord(
                batch,
                0,
                len,
                (p, l) -> {
                    byte[] bin = new byte[l];
                    batch.getBytes(p, bin);
                    decoded.add(OrderRecordCodec.decodeFields(bin));
                });
        assertEquals(3, decoded.size(), "逐条回读");
        for (int i = 0; i < 3; i++) {
            SnapshotOrderFields src = orders.get(i);
            SnapshotOrderFields d = decoded.get(i);
            assertEquals(src.orderId, d.orderId);
            assertEquals(src.userId, d.userId);
            assertEquals(SYMBOL, d.symbol);
            assertEquals(src.price, d.price);
            assertEquals(src.origQty, d.origQty);
            assertEquals(src.direction, d.direction);
            assertEquals(src.tif, d.tif);
            assertEquals(src.time, d.time);
        }
    }

    @Test
    void triggerBatchRoundTrip() {
        List<TriggerSnapshotFields> triggers =
                List.of(
                        new TriggerSnapshotFields(
                                1L,
                                10L,
                                p(50100),
                                p(50200),
                                p(1.0),
                                0,
                                com.ocx.book.TriggerSlots.TYPE_STOP_LIMIT,
                                0L,
                                1787310000000L),
                        new TriggerSnapshotFields(
                                2L,
                                20L,
                                p(49900),
                                0L,
                                p(2.0),
                                1,
                                com.ocx.book.TriggerSlots.TYPE_STOP_MARKET,
                                0L,
                                1787310001000L),
                        new TriggerSnapshotFields(
                                3L,
                                30L,
                                p(50500),
                                p(50600),
                                p(0.5),
                                1,
                                com.ocx.book.TriggerSlots.TYPE_TAKE_PROFIT_LIMIT,
                                0L,
                                1787310002000L));

        UnsafeBuffer batch = new UnsafeBuffer(new byte[SnapshotCodec.MAX_BATCH_BYTES]);
        UnsafeBuffer rec =
                new UnsafeBuffer(
                        new byte[OrderRecordCodec.MAX_ENCODED_LENGTH + OrderRecordCodec.MAGIC_LEN]);
        byte[] strScratch =
                new byte[OrderRecordCodec.MAX_ENCODED_LENGTH + OrderRecordCodec.MAGIC_LEN];
        MessageHeaderEncoder header = new MessageHeaderEncoder();
        OrderCreateEncoder enc = new OrderCreateEncoder();
        int len = SnapshotCodec.encodeBatchHeader(batch, 0, SnapshotCodec.TYPE_STOP_BATCH, 0);
        int count = 0;
        for (TriggerSnapshotFields s : triggers) {
            int recLen =
                    OrderRecordCodec.encodeStopFields(
                            SYMBOL,
                            s.orderId(),
                            s.userId(),
                            s.triggerPrice(),
                            s.price(),
                            s.origQty(),
                            s.direction(),
                            s.triggerType(),
                            s.trailDelta(),
                            s.time(),
                            rec,
                            0,
                            strScratch,
                            header,
                            enc);
            len = SnapshotCodec.appendRecord(batch, 0, len, rec, 0, recLen);
            count++;
        }
        SnapshotCodec.encodeBatchHeader(batch, 0, SnapshotCodec.TYPE_STOP_BATCH, count);

        List<TriggerSnapshotFields> decoded = new ArrayList<>();
        SnapshotCodec.forEachRecord(
                batch,
                0,
                len,
                (p, l) -> {
                    byte[] bin = new byte[l];
                    batch.getBytes(p, bin);
                    decoded.add(OrderRecordCodec.decodeStopFields(bin));
                });
        assertEquals(3, decoded.size(), "条件单逐条回读");
        for (int i = 0; i < 3; i++) {
            TriggerSnapshotFields src = triggers.get(i);
            TriggerSnapshotFields d = decoded.get(i);
            assertEquals(src.orderId(), d.orderId());
            assertEquals(src.userId(), d.userId());
            assertEquals(src.triggerPrice(), d.triggerPrice(), "触发价");
            assertEquals(src.price(), d.price(), "限价（MARKET 类为 0）");
            assertEquals(src.origQty(), d.origQty());
            assertEquals(src.direction(), d.direction());
            assertEquals(src.triggerType(), d.triggerType(), "条件单类型");
            assertEquals(src.trailDelta(), d.trailDelta(), "移动止损偏移");
            assertEquals(src.time(), d.time());
        }
    }

    /** 批量帧超限必须返回 -1（调用方 flush 后重试），不得静默截断记录。 */
    @Test
    void appendRecordRejectsOverflow() {
        UnsafeBuffer batch = new UnsafeBuffer(new byte[SnapshotCodec.MAX_BATCH_BYTES]);
        UnsafeBuffer rec =
                new UnsafeBuffer(
                        new byte[OrderRecordCodec.MAX_ENCODED_LENGTH + OrderRecordCodec.MAGIC_LEN]);
        byte[] strScratch =
                new byte[OrderRecordCodec.MAX_ENCODED_LENGTH + OrderRecordCodec.MAGIC_LEN];
        MessageHeaderEncoder header = new MessageHeaderEncoder();
        OrderCreateEncoder enc = new OrderCreateEncoder();
        int len = SnapshotCodec.encodeBatchHeader(batch, 0, SnapshotCodec.TYPE_ORDER_BATCH, 0);
        int count = 0;
        while (true) {
            int recLen =
                    OrderRecordCodec.encodeFields(
                            SYMBOL,
                            count + 1L,
                            7L,
                            p(70000),
                            p(1.0),
                            0,
                            0,
                            1787310000000L,
                            0L,
                            0L,
                            rec,
                            0,
                            strScratch,
                            header,
                            enc);
            int next = SnapshotCodec.appendRecord(batch, 0, len, rec, 0, recLen);
            if (next < 0) {
                break;
            }
            len = next;
            count++;
        }
        assertTrue(count >= 10, "小批量帧应容纳多条记录，实际 " + count);
        assertTrue(len < SnapshotCodec.MAX_BATCH_BYTES, "flush 前不超上限");
        assertFalse(SnapshotCodec.hasMagic(new UnsafeBuffer(new byte[8]), 0), "空缓冲无魔数");
    }
}
