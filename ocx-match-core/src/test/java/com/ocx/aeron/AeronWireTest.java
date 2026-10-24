package com.ocx.aeron;

import static org.junit.jupiter.api.Assertions.*;

import com.ocx.book.MatchFill;
import com.ocx.book.ReusableFillList;
import com.ocx.codec.IoCodec;
import com.ocx.codec.domain.OrderIn;
import com.ocx.codec.domain.OrderInFields;
import com.ocx.math.Decimal;
import com.ocx.metrics.JvmMetrics;
import com.ocx.metrics.MetricsSnapshot;
import com.ocx.outbound.OutboundDepth;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

class AeronWireTest {

    private static final int BUF_SIZE = 4096;

    @Test
    void inboundRowRoundTrip() {
        UnsafeBuffer buf = new UnsafeBuffer(new byte[AeronWire.INBOUND_ROW_SIZE]);
        OrderIn src =
                new OrderIn(
                        123L,
                        456L,
                        "BTCUSDT",
                        Decimal.parse("70000"),
                        Decimal.parse("1.5"),
                        Decimal.ZERO,
                        null,
                        (byte) 0,
                        (byte) 1,
                        "GTC",
                        (byte) 0,
                        null,
                        null);
        byte[] frame = IoCodec.encodeOrderIn(src);
        assertTrue(frame.length <= AeronWire.INBOUND_FRAME_LENGTH, "SBE 帧须落在入站行帧区内");

        AeronWire.encodeInboundFrame(buf, new UnsafeBuffer(frame), 0, frame.length, 2024L);
        OrderInFields out = new OrderInFields();
        long ts = AeronWire.readInboundTimestamp(buf, 0);
        IoCodec.readOrderIn(
                IoCodec.context(),
                buf,
                AeronWire.INBOUND_TS_LENGTH,
                AeronWire.INBOUND_FRAME_LENGTH,
                out);

        OrderIn dto = IoCodec.decodeOrderIn(frame);
        assertEquals(dto.id(), out.id);
        assertEquals(dto.userId(), out.userId);
        assertEquals(dto.price().mantissa(), out.priceMantissa);
        assertEquals(dto.price().exponent(), out.priceExponent);
        assertEquals(dto.origQty().mantissa(), out.origQtyMantissa);
        assertEquals(dto.origQty().exponent(), out.origQtyExponent);
        assertEquals(dto.side(), out.side);
        assertEquals(dto.type(), out.type);
        assertEquals(IoCodec.TIF_GTC, out.timeInForce);
        assertEquals(2024L, ts);
    }

    /** 变长入站行回归（2026-09-01 录制端解耦专项）：发布端按实际帧长 offer，
     *  读取端按消息实际长度解码——短行（101B 级）不得丢字段、不得读到填充尾。 */
    @Test
    void inboundVariableLengthRowRoundTrip() {
        UnsafeBuffer buf = new UnsafeBuffer(new byte[AeronWire.INBOUND_ROW_SIZE]);
        OrderIn src =
                new OrderIn(
                        777L,
                        888L,
                        "BTCUSDT",
                        Decimal.parse("12345.67"),
                        Decimal.parse("0.25"),
                        Decimal.ZERO,
                        null,
                        (byte) 1,
                        (byte) 2,
                        "FOK",
                        (byte) 1,
                        null,
                        null);
        byte[] frame = IoCodec.encodeOrderIn(src);
        assertTrue(
                frame.length < AeronWire.INBOUND_FRAME_LENGTH,
                "用例须构造短帧以覆盖变长路径（当前 " + frame.length + "B）");

        // 发布端（AeronOrderInSink 语义）：8B 时刻 + 实际帧长，不补零
        AeronWire.encodeInboundFrame(buf, new UnsafeBuffer(frame), 0, frame.length, 424242L);
        int offeredLen = AeronWire.INBOUND_TS_LENGTH + frame.length;
        // 篡改填充尾（旧定长路径会误读这些字节）
        for (int i = offeredLen; i < AeronWire.INBOUND_ROW_SIZE; i++) {
            buf.putByte(i, (byte) 0x5A);
        }

        // 读取端（AeronOrderInSource 语义）：按消息实际长度解码
        OrderInFields out = new OrderInFields();
        long ts = AeronWire.readInboundTimestamp(buf, 0);
        IoCodec.readOrderIn(
                IoCodec.context(),
                buf,
                AeronWire.INBOUND_TS_LENGTH,
                Math.min(offeredLen - AeronWire.INBOUND_TS_LENGTH, AeronWire.INBOUND_FRAME_LENGTH),
                out);
        assertEquals(424242L, ts);
        assertEquals(src.id(), out.id);
        assertEquals(src.userId(), out.userId);
        assertEquals(src.price().mantissa(), out.priceMantissa);
        assertEquals(src.origQty().mantissa(), out.origQtyMantissa);
        assertEquals(src.side(), out.side);
        assertEquals(src.type(), out.type);
        assertEquals(IoCodec.TIF_FOK, out.timeInForce);

        // 兼容：旧定长 264B 行仍可解码（读取端只收窄长度，不改变布局）
        UnsafeBuffer fixed = new UnsafeBuffer(new byte[AeronWire.INBOUND_ROW_SIZE]);
        AeronWire.encodeInboundFrame(fixed, new UnsafeBuffer(frame), 0, frame.length, 1L);
        OrderInFields fixedOut = new OrderInFields();
        IoCodec.readOrderIn(
                IoCodec.context(),
                fixed,
                AeronWire.INBOUND_TS_LENGTH,
                AeronWire.INBOUND_FRAME_LENGTH,
                fixedOut);
        assertEquals(src.id(), fixedOut.id);
    }

    @Test
    void fillRowRoundTrip() {
        UnsafeBuffer buf = new UnsafeBuffer(new byte[BUF_SIZE]);
        MatchFill src = new MatchFill();
        src.direction = 1;
        src.symbol = "BTCUSDT";
        src.takerOrderId = 11L;
        src.makerOrderId = 22L;
        src.price = 70000_00000000L;
        src.priceExponent = -8;
        src.qty = 150_000000L;
        src.qtyExponent = -8;
        src.takerCumQuote = 999L;
        src.makerCumQuote = 888L;
        src.turnOverExponent = -8;
        src.time = 1234L;

        int len = AeronWire.encodeFill(buf, src, AeronWire.symbolBytes("BTCUSDT"));
        MatchFill out = new MatchFill();
        AeronWire.decodeFill(buf, 0, out);

        assertEquals(1, out.direction);
        assertEquals("BTCUSDT", out.symbol);
        assertEquals(11L, out.takerOrderId);
        assertEquals(22L, out.makerOrderId);
        assertEquals(70000_00000000L, out.price);
        assertEquals(150_000000L, out.qty);
        assertEquals(999L, out.takerCumQuote);
        assertEquals(888L, out.makerCumQuote);
        assertEquals(1234L, out.time);
        assertTrue(len > 0 && len <= 128);
    }

    @Test
    void fillBatchRoundTrip() {
        UnsafeBuffer buf = new UnsafeBuffer(new byte[BUF_SIZE]);
        byte[] symbol = AeronWire.symbolBytes("BTCUSDT");
        int count = 40;
        ReusableFillList fills = new ReusableFillList();
        for (int i = 0; i < count; i++) {
            MatchFill f = new MatchFill();
            f.symbol = "BTCUSDT";
            f.direction = i % 2;
            f.takerOrderId = 1000L + i;
            f.makerOrderId = 2000L + i;
            f.price = 70000_00000000L + i;
            f.qty = 100_000000L;
            f.time = i;
            fills.add(f);
        }

        int len = AeronWire.encodeFillBatch(buf, fills, symbol, 0, count);
        assertEquals(AeronWire.TYPE_FILL_BATCH, buf.getByte(0));
        assertEquals(count, buf.getInt(1));
        assertEquals(5 + count * 96, len);

        MatchFill out = new MatchFill();
        for (int i = 0; i < count; i++) {
            AeronWire.decodeFill(buf, 5 + i * 96, out);
            assertEquals(1000L + i, out.takerOrderId);
            assertEquals(2000L + i, out.makerOrderId);
            assertEquals(70000_00000000L + i, out.price);
            assertEquals(i % 2, out.direction);
        }
    }

    @Test
    void depthRowRoundTrip() {
        UnsafeBuffer buf = new UnsafeBuffer(new byte[BUF_SIZE]);
        OutboundDepth src = new OutboundDepth();
        src.symbol = "BTCUSDT";
        src.bidCount = 3;
        src.askCount = 2;
        src.ts = 42L;
        src.priceScale = 8;
        src.volumeScale = 8;
        src.bidPrices[0] = 70000L;
        src.bidPrices[1] = 69900L;
        src.askPrices[0] = 70100L;
        src.bidVolumes[1] = 500L;
        src.askVolumes[0] = 300L;

        int len = AeronWire.encodeDepth(buf, src, AeronWire.symbolBytes("BTCUSDT"));
        OutboundDepth out = new OutboundDepth();
        AeronWire.decodeDepth(buf, 0, out);

        assertEquals("BTCUSDT", out.symbol);
        assertEquals(3, out.bidCount);
        assertEquals(2, out.askCount);
        assertEquals(42L, out.ts);
        assertEquals(70000L, out.bidPrices[0]);
        assertEquals(69900L, out.bidPrices[1]);
        assertEquals(500L, out.bidVolumes[1]);
        assertEquals(70100L, out.askPrices[0]);
        assertEquals(300L, out.askVolumes[0]);
        assertTrue(len <= BUF_SIZE);
    }

    @Test
    void eventRowTruncatesDetail() {
        UnsafeBuffer buf = new UnsafeBuffer(new byte[BUF_SIZE]);
        String longDetail = "x".repeat(500);
        AeronWire.encodeEvent(
                buf, AeronWire.symbolBytes("BTCUSDT"), "wal_append_failed", longDetail);
        AeronWire.EventFields out = new AeronWire.EventFields();
        AeronWire.decodeEvent(buf, 0, out);
        assertEquals("BTCUSDT", out.symbol);
        assertEquals("wal_append_failed", out.event);
        assertEquals(160, out.detail.length());

        AeronWire.encodeEvent(buf, AeronWire.symbolBytes(null), "emit_all", null);
        AeronWire.decodeEvent(buf, 0, out);
        assertEquals("", out.symbol);
        assertEquals("emit_all", out.event);
        assertEquals("", out.detail);
    }

    @Test
    void metricsRowRoundTrip() {
        UnsafeBuffer buf = new UnsafeBuffer(new byte[BUF_SIZE]);
        MetricsSnapshot src =
                new MetricsSnapshot(
                        "BTCUSDT",
                        1000L,
                        900L,
                        true,
                        42L,
                        12345L,
                        99.5,
                        10L,
                        5L,
                        30L,
                        2L,
                        1L,
                        50L,
                        100000L,
                        7,
                        8,
                        1.5,
                        2.5,
                        3.5,
                        4.5,
                        9.9,
                        128L,
                        new JvmMetrics(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L),
                        2_570_684L,
                        777L,
                        1_500_000L,
                        1_070_684L);

        AeronWire.encodeMetrics(buf, src, AeronWire.symbolBytes("BTCUSDT"));
        MetricsSnapshot out = AeronWire.decodeMetrics(buf, 0);

        assertEquals("BTCUSDT", out.symbol());
        assertEquals(1000L, out.exportTs());
        assertEquals(900L, out.lastOrderTs());
        assertTrue(out.overloaded());
        assertEquals(42L, out.consumerLag());
        assertEquals(12345L, out.inboundBacklog());
        assertEquals(777L, out.outboundBackpressure());
        assertEquals(1_500_000L, out.buyOrders());
        assertEquals(1_070_684L, out.sellOrders());
        assertEquals(99.5, out.throughput());
        assertEquals(10L, out.ordersLimit());
        assertEquals(30L, out.tradesTotal());
        assertEquals(7, out.depthBid());
        assertEquals(9.9, out.durationMax());
        assertEquals(128L, out.batchSizeMax());
        assertEquals(11L, out.jvm().uptimeMillis());
        assertEquals(2_570_684L, out.slotsUsed());
    }

    @Test
    void ingressMetricsRowRoundTrip() {
        UnsafeBuffer buf = new UnsafeBuffer(new byte[AeronWire.INGRESS_METRICS_SIZE]);
        int len = AeronWire.encodeIngressMetrics(buf, 12_345L, 512_000.5, 9_999L);

        assertEquals(AeronWire.INGRESS_METRICS_SIZE, len);
        AeronWire.IngressMetricsRow out = AeronWire.decodeIngressMetrics(buf, 0);

        assertEquals(12_345L, out.consumerLag());
        assertEquals(512_000.5, out.currentTps(), 1e-9);
        assertEquals(9_999L, out.rejectedTotal());
        assertEquals(AeronWire.TYPE_INGRESS_METRICS, buf.getByte(0));
    }
}
