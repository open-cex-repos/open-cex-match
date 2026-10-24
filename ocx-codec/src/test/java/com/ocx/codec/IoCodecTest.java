package com.ocx.codec;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ocx.codec.domain.DepthItemVo;
import com.ocx.codec.domain.DepthSnapshotMessage;
import com.ocx.codec.domain.ExchangeTrade;
import com.ocx.codec.domain.OrderIn;
import com.ocx.codec.domain.OrderInFields;
import com.ocx.codec.enums.OrderDirection;
import com.ocx.math.Decimal;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

class IoCodecTest {

    @Test
    void roundtripLimitOrder() {
        OrderIn src =
                order(
                        123456L,
                        990001L,
                        "BTCUSDT",
                        new BigDecimal("70000.5"),
                        new BigDecimal("1.25"),
                        BigDecimal.ZERO,
                        new BigDecimal("69900"),
                        (byte) 0,
                        (byte) 1,
                        "GTC",
                        (byte) 0,
                        LocalDateTime.of(2026, 8, 21, 23, 0, 0));

        byte[] encoded = IoCodec.encodeOrderIn(src);
        assertTrue(IoCodec.isSbe(encoded));

        OrderIn dst = IoCodec.decodeOrderIn(encoded);
        assertEquals(src.id(), dst.id());
        assertEquals(src.userId(), dst.userId());
        assertEquals(src.symbol(), dst.symbol());
        assertEquals(0, src.price().compareTo(dst.price()));
        assertEquals(0, src.origQty().compareTo(dst.origQty()));
        assertEquals(0, src.executedQty().compareTo(dst.executedQty()));
        assertEquals(0, src.mergeLowPrice().compareTo(dst.mergeLowPrice()));
        assertEquals(src.side(), dst.side());
        assertEquals(src.type(), dst.type());
        assertEquals(src.timeInForce(), dst.timeInForce());
        assertEquals(src.status(), dst.status());
        assertEquals(src.createTime(), dst.createTime());
    }

    @Test
    void roundtripMarketOrderWithNulls() {
        OrderIn src =
                order(
                        200L,
                        990002L,
                        "ETHUSDT",
                        null,
                        new BigDecimal("2"),
                        null,
                        null,
                        (byte) 1,
                        (byte) 2,
                        null,
                        null,
                        null);

        OrderIn dst = IoCodec.decodeOrderIn(IoCodec.encodeOrderIn(src));
        assertEquals(src.id(), dst.id());
        assertNull(dst.price());
        assertEquals(0, src.origQty().compareTo(dst.origQty()));
        assertEquals(
                0, Decimal.ZERO.compareTo(dst.executedQty()), "executedQty 缺省/0 统一解码为 ZERO（合法业务值）");
        assertNull(dst.mergeLowPrice());
        assertEquals(src.side(), dst.side());
        assertEquals(src.type(), dst.type());
        assertEquals("GTC", dst.timeInForce(), "TIF 缺省统一编码为 uint8 0=GTC");
        assertEquals((byte) 0, dst.status(), "status=0 是合法业务值");
        assertNull(dst.createTime());
    }

    /** byte[] 与 ByteBuffer 两条解码路径结果逐字段等价（新 schema：定长 symbol + uint8 枚举）。 */
    @Test
    void decodeOrderInByteArray与ByteBuffer路径等价() {
        OrderIn src =
                order(
                        555L,
                        77L,
                        "BTCUSDT",
                        new BigDecimal("123.45"),
                        new BigDecimal("2.5"),
                        BigDecimal.ZERO,
                        new BigDecimal("120"),
                        (byte) 0,
                        (byte) 1,
                        "GTC",
                        (byte) 0,
                        LocalDateTime.of(2026, 8, 21, 23, 0, 0));

        byte[] encoded = IoCodec.encodeOrderIn(src);

        OrderIn fromBytes = IoCodec.decodeOrderIn(encoded);
        OrderIn fromBuffer = IoCodec.decodeOrderIn(ByteBuffer.wrap(encoded));

        assertEquals(src.id(), fromBytes.id());
        assertEquals(src.userId(), fromBytes.userId());
        assertEquals(src.symbol(), fromBytes.symbol());
        assertEquals(0, src.price().compareTo(fromBytes.price()));
        assertEquals(0, src.origQty().compareTo(fromBytes.origQty()));
        assertEquals(0, src.executedQty().compareTo(fromBytes.executedQty()));
        assertEquals(0, src.mergeLowPrice().compareTo(fromBytes.mergeLowPrice()));
        assertEquals(src.side(), fromBytes.side());
        assertEquals(src.type(), fromBytes.type());
        assertEquals(src.timeInForce(), fromBytes.timeInForce());
        assertEquals(src.status(), fromBytes.status());

        // 两条路径解码结果完全一致
        assertEquals(fromBytes.id(), fromBuffer.id());
        assertEquals(fromBytes.symbol(), fromBuffer.symbol());
        assertEquals(fromBytes.price(), fromBuffer.price());
        assertEquals(fromBytes.origQty(), fromBuffer.origQty());
        assertEquals(fromBytes.timeInForce(), fromBuffer.timeInForce());
        assertEquals(fromBytes.createTime(), fromBuffer.createTime());
    }

    @Test
    void fixedSymbolAndTifEncoding() {
        OrderIn src =
                order(
                        300L,
                        7L,
                        "CACHEUSDT",
                        new BigDecimal("50000"),
                        new BigDecimal("0.5"),
                        BigDecimal.ZERO,
                        null,
                        (byte) 0,
                        (byte) 1,
                        "FOK",
                        (byte) 0,
                        LocalDateTime.of(2026, 8, 24, 10, 0, 0));

        byte[] encoded = IoCodec.encodeOrderIn(src);
        OrderIn a = IoCodec.decodeOrderIn(encoded);
        assertEquals("CACHEUSDT", a.symbol());
        assertEquals("FOK", a.timeInForce());

        // 定长 symbol 右补零：解码必须 trim 零填充，且不同取值解码正确
        OrderIn c =
                IoCodec.decodeOrderIn(
                        IoCodec.encodeOrderIn(
                                order(
                                        300L,
                                        7L,
                                        "OTHERUSDT",
                                        new BigDecimal("50000"),
                                        new BigDecimal("0.5"),
                                        BigDecimal.ZERO,
                                        null,
                                        (byte) 0,
                                        (byte) 1,
                                        "IOC",
                                        (byte) 0,
                                        LocalDateTime.of(2026, 8, 24, 10, 0, 0))));
        assertEquals("OTHERUSDT", c.symbol());
        assertEquals("IOC", c.timeInForce());
        assertEquals(16, IoCodec.ORDER_IN_SYMBOL_LENGTH, "定长 16 字节路由键");
        assertTrue(c.symbol() != a.symbol());
    }

    /** 撮合热路径：readOrderIn 原生字段直读与 DTO 解码逐字段等价，零 String/Decimal 中间态。 */
    @Test
    void readOrderInNativeFieldsEquivalentToDto() {
        OrderIn src =
                order(
                        888L,
                        4242L,
                        "ETHUSDT",
                        new BigDecimal("3500.25"),
                        new BigDecimal("2.5"),
                        new BigDecimal("1.0"),
                        new BigDecimal("3400"),
                        (byte) 1,
                        (byte) 3,
                        "FOK",
                        (byte) 5,
                        LocalDateTime.of(2026, 8, 25, 12, 0, 0));

        byte[] frame = IoCodec.encodeOrderIn(src);
        OrderInFields f = new OrderInFields();
        IoCodec.readOrderIn(frame, frame.length, f);

        OrderIn dto = IoCodec.decodeOrderIn(frame);
        assertEquals(dto.id(), f.id);
        assertEquals(dto.userId(), f.userId);
        assertEquals(dto.price().mantissa(), f.priceMantissa);
        assertEquals(dto.price().exponent(), f.priceExponent);
        assertEquals(dto.origQty().mantissa(), f.origQtyMantissa);
        assertEquals(dto.origQty().exponent(), f.origQtyExponent);
        assertEquals(dto.executedQty().mantissa(), f.executedQtyMantissa);
        assertEquals(dto.executedQty().exponent(), f.executedQtyExponent);
        assertEquals(dto.mergeLowPrice().mantissa(), f.mergeLowPriceMantissa);
        assertEquals(dto.mergeLowPrice().exponent(), f.mergeLowPriceExponent);
        assertEquals((byte) 1, f.side);
        assertEquals((byte) 3, f.type);
        assertEquals(IoCodec.TIF_FOK, f.timeInForce);
        assertEquals((byte) 5, f.status);
        assertTrue(f.createTimeMillis > 0);
    }

    @Test
    void decimalOfReusesCachedInstances() {
        assertSame(Decimal.of(5000000L, -2), Decimal.of(5000000L, -2));
        assertSame(Decimal.ZERO, Decimal.of(0L, -8));
        assertEquals(new BigDecimal("50000.00"), Decimal.of(5000000L, -2).toBigDecimal());
    }

    @Test
    void roundtripExchangeTrade() {
        ExchangeTrade src = new ExchangeTrade();
        src.setSymbol("BTCUSDT");
        src.setDirection(OrderDirection.BUY);
        setDecimal(src::setPrice, "70000.25");
        setDecimal(src::setAmount, "0.5");
        src.setBuyOrderId(1001L);
        src.setSellOrderId(2002L);
        setDecimal(src::setBuyTurnover, "35000.125");
        setDecimal(src::setSellTurnover, "35000.125");
        src.setTime(1787310000000L);

        ExchangeTrade dst = IoCodec.decodeExchangeTrade(IoCodec.encodeExchangeTrade(src));
        assertEquals(src.getSymbol(), dst.getSymbol());
        assertEquals(src.getDirection(), dst.getDirection());
        assertEquals(src.getPriceMantissa(), dst.getPriceMantissa());
        assertEquals(src.getPriceExponent(), dst.getPriceExponent());
        assertEquals(src.getAmountMantissa(), dst.getAmountMantissa());
        assertEquals(src.getAmountExponent(), dst.getAmountExponent());
        assertEquals(src.getBuyOrderId(), dst.getBuyOrderId());
        assertEquals(src.getSellOrderId(), dst.getSellOrderId());
        assertEquals(src.getBuyTurnoverMantissa(), dst.getBuyTurnoverMantissa());
        assertEquals(src.getBuyTurnoverExponent(), dst.getBuyTurnoverExponent());
        assertEquals(src.getSellTurnoverMantissa(), dst.getSellTurnoverMantissa());
        assertEquals(src.getSellTurnoverExponent(), dst.getSellTurnoverExponent());
        assertEquals(src.getTime(), dst.getTime());
    }

    /** egress 热路径：缓存 ctx + 定长 symbol 字节版与 String 版输出逐字节一致。 */
    @Test
    void tradeFieldsByteSymbolMatchesStringVersion() {
        byte[] bufA = new byte[IoCodec.FRAME_LENGTH_PREFIX + IoCodec.MAX_TRADE_FRAME_LENGTH];
        byte[] bufB = new byte[IoCodec.FRAME_LENGTH_PREFIX + IoCodec.MAX_TRADE_FRAME_LENGTH];
        IoCodec.CodecContext ctx = IoCodec.context();

        int lenA =
                IoCodec.encodeTradeFields(
                        "BTCUSDT",
                        0,
                        70000250000000L,
                        -8,
                        50000000L,
                        -8,
                        1001L,
                        2002L,
                        35000125000000L,
                        -8,
                        35000125000000L,
                        -8,
                        1787310000000L,
                        bufA,
                        IoCodec.FRAME_LENGTH_PREFIX);
        int lenB =
                IoCodec.encodeTradeFields(
                        ctx,
                        IoCodec.symbolKey("BTCUSDT"),
                        0,
                        70000250000000L,
                        -8,
                        50000000L,
                        -8,
                        1001L,
                        2002L,
                        35000125000000L,
                        -8,
                        35000125000000L,
                        -8,
                        1787310000000L,
                        bufB,
                        IoCodec.FRAME_LENGTH_PREFIX);

        assertEquals(lenA, lenB);
        assertArrayEquals(bufA, bufB, "byte[] symbol 版输出必须与 String 版完全一致");
    }

    @Test
    void batchRoundtripExchangeTrades() {
        List<ExchangeTrade> srcList = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            ExchangeTrade src = new ExchangeTrade();
            src.setSymbol("BTCUSDT");
            src.setDirection(i % 2 == 0 ? OrderDirection.BUY : OrderDirection.SELL);
            setDecimal(src::setPrice, "70000.25");
            setDecimal(src::setAmount, "0.5");
            src.setBuyOrderId(1001L + i);
            src.setSellOrderId(2002L + i);
            setDecimal(src::setBuyTurnover, "35000.125");
            setDecimal(src::setSellTurnover, "35000.125");
            src.setTime(1787310000000L + i);
            srcList.add(src);
        }

        byte[] buf =
                new byte
                        [srcList.size()
                                * (IoCodec.FRAME_LENGTH_PREFIX + IoCodec.MAX_TRADE_FRAME_LENGTH)];
        int len = IoCodec.encodeExchangeTradeBatch(srcList, buf);
        assertTrue(len > 0, "批量帧编码长度应为正");

        byte[] payload = new byte[len];
        System.arraycopy(buf, 0, payload, 0, len);

        List<ExchangeTrade> dstList = new ArrayList<>();
        IoCodec.forEachExchangeTrade(payload, dstList::add);

        assertEquals(srcList.size(), dstList.size(), "批量帧应解码出全部成交");
        for (int i = 0; i < srcList.size(); i++) {
            ExchangeTrade src = srcList.get(i);
            ExchangeTrade dst = dstList.get(i);
            assertEquals(src.getSymbol(), dst.getSymbol());
            assertEquals(src.getDirection(), dst.getDirection());
            assertEquals(src.getPriceMantissa(), dst.getPriceMantissa());
            assertEquals(src.getPriceExponent(), dst.getPriceExponent());
            assertEquals(src.getAmountMantissa(), dst.getAmountMantissa());
            assertEquals(src.getAmountExponent(), dst.getAmountExponent());
            assertEquals(src.getBuyOrderId(), dst.getBuyOrderId());
            assertEquals(src.getSellOrderId(), dst.getSellOrderId());
            assertEquals(src.getBuyTurnoverMantissa(), dst.getBuyTurnoverMantissa());
            assertEquals(src.getBuyTurnoverExponent(), dst.getBuyTurnoverExponent());
            assertEquals(src.getSellTurnoverMantissa(), dst.getSellTurnoverMantissa());
            assertEquals(src.getSellTurnoverExponent(), dst.getSellTurnoverExponent());
            assertEquals(src.getTime(), dst.getTime());
        }
    }

    @Test
    void forEachExchangeTrade_兼容旧单帧() {
        ExchangeTrade src = new ExchangeTrade();
        src.setSymbol("BTCUSDT");
        src.setDirection(OrderDirection.BUY);
        setDecimal(src::setPrice, "70000.25");
        setDecimal(src::setAmount, "0.5");
        src.setBuyOrderId(1001L);
        src.setSellOrderId(2002L);
        setDecimal(src::setBuyTurnover, "35000.125");
        setDecimal(src::setSellTurnover, "35000.125");
        src.setTime(1787310000000L);

        List<ExchangeTrade> dstList = new ArrayList<>();
        IoCodec.forEachExchangeTrade(IoCodec.encodeExchangeTrade(src), dstList::add);

        assertEquals(1, dstList.size(), "旧单帧（OCX2 魔数开头）应解析为一笔");
        assertEquals(src.getBuyOrderId(), dstList.get(0).getBuyOrderId());
    }

    @Test
    void roundtripDepthSnapshotPrimitiveArrays() {
        long[] askPrices = {7000025000000L, 7000100000000L};
        long[] askVolumes = {125000000L, 200000000L};
        long[] bidPrices = {6999900000000L};
        long[] bidVolumes = {50000000L};

        byte[] payload =
                IoCodec.encodeDepthSnapshot(
                        "BTCUSDT",
                        1787310000000L,
                        8,
                        8,
                        askPrices,
                        askVolumes,
                        2,
                        bidPrices,
                        bidVolumes,
                        1);
        DepthSnapshotMessage dst = IoCodec.decodeDepthSnapshot(payload);
        assertEquals("BTCUSDT", dst.symbol());
        assertEquals(2, dst.asks().size());
        assertEquals(1, dst.bids().size());
        assertEquals(0, Decimal.parse("70000.25").compareTo(dst.asks().get(0).price()));
        assertEquals(0, Decimal.parse("1.25").compareTo(dst.asks().get(0).volume()));
        assertEquals(0, Decimal.parse("69999").compareTo(dst.bids().get(0).price()));
        assertEquals(0, Decimal.parse("0.5").compareTo(dst.bids().get(0).volume()));
    }

    @Test
    void roundtripZeroUserIdPreserved() {
        OrderIn src =
                order(
                        300L,
                        0L,
                        "BTCUSDT",
                        null,
                        new BigDecimal("1"),
                        null,
                        null,
                        (byte) 0,
                        (byte) 1,
                        null,
                        null,
                        null);

        OrderIn dst = IoCodec.decodeOrderIn(IoCodec.encodeOrderIn(src));
        assertEquals(0L, dst.userId());
    }

    /** 按类型解码（2026-08-29）：撤单（status=2）只读 status+id+userId，订单字段裁剪为零（防槽位残留误判）。 */
    @Test
    void decodeByTypeCancelTrimsOrderFields() {
        OrderIn src =
                order(
                        123456L,
                        990001L,
                        "BTCUSDT",
                        new BigDecimal("70000.5"),
                        new BigDecimal("1.25"),
                        BigDecimal.ZERO,
                        new BigDecimal("69900"),
                        (byte) 0,
                        (byte) 1,
                        "GTC",
                        (byte) 2,
                        LocalDateTime.of(2026, 8, 21, 23, 0, 0));
        OrderInFields out = new OrderInFields();
        byte[] enc = IoCodec.encodeOrderIn(src);
        IoCodec.readOrderIn(enc, enc.length, out);
        assertEquals(123456L, out.id);
        assertEquals(990001L, out.userId);
        assertEquals((byte) 2, out.status);
        assertEquals(0L, out.priceMantissa, "撤单分支价格裁剪为零");
        assertEquals(0L, out.origQtyMantissa, "撤单分支数量裁剪为零");
        assertEquals(0L, out.executedQtyMantissa, "撤单分支已成交量裁剪为零");
        assertEquals(0L, out.mergeLowPriceMantissa, "撤单分支触发价裁剪为零");
        assertEquals((byte) 0, out.side, "撤单分支方向裁剪为零");
        assertEquals((byte) 0, out.type, "撤单分支类型裁剪为零");
        assertEquals((byte) 0, out.timeInForce, "撤单分支 TIF 裁剪为零");
        assertEquals(0L, out.createTimeMillis, "撤单分支时间裁剪为零");
        assertEquals(0L, out.visibleQtyMantissa, "撤单分支冰山量裁剪为零");
    }

    /** 按类型解码（2026-08-29）：批量撤单（status=99）只读 status+userId，订单字段裁剪为零。 */
    @Test
    void decodeByTypeBatchCancelTrimsOrderFields() {
        OrderIn src =
                order(
                        1L,
                        42L,
                        "BTCUSDT",
                        new BigDecimal("70000"),
                        new BigDecimal("1"),
                        BigDecimal.ZERO,
                        null,
                        (byte) 0,
                        (byte) 1,
                        "GTC",
                        (byte) 99,
                        null);
        OrderInFields out = new OrderInFields();
        byte[] enc = IoCodec.encodeOrderIn(src);
        IoCodec.readOrderIn(enc, enc.length, out);
        assertEquals(42L, out.userId);
        assertEquals((byte) 99, out.status);
        assertEquals(0L, out.priceMantissa, "批撤分支价格裁剪为零");
        assertEquals(0L, out.origQtyMantissa, "批撤分支数量裁剪为零");
        assertEquals((byte) 0, out.side, "批撤分支方向裁剪为零");
        assertEquals((byte) 0, out.type, "批撤分支类型裁剪为零");
        assertEquals(0L, out.createTimeMillis, "批撤分支时间裁剪为零");
    }

    /** 按类型解码（2026-08-29）：普通单仍完整解码全部字段，与对象版路径逐字段等价。 */
    @Test
    void decodeByTypeFullOrderKeepsAllFields() {
        OrderIn src =
                order(
                        555L,
                        77L,
                        "BTCUSDT",
                        new BigDecimal("123.45"),
                        new BigDecimal("2.5"),
                        BigDecimal.ZERO,
                        new BigDecimal("120"),
                        (byte) 0,
                        (byte) 1,
                        "GTC",
                        (byte) 0,
                        LocalDateTime.of(2026, 8, 21, 23, 0, 0));
        OrderIn dst = IoCodec.decodeOrderIn(IoCodec.encodeOrderIn(src));
        OrderInFields out = new OrderInFields();
        byte[] enc = IoCodec.encodeOrderIn(src);
        IoCodec.readOrderIn(enc, enc.length, out);
        assertEquals(dst.id(), out.id);
        assertEquals(dst.userId(), out.userId);
        assertEquals(dst.price().mantissa(), out.priceMantissa);
        assertEquals(dst.price().exponent(), out.priceExponent);
        assertEquals(dst.origQty().mantissa(), out.origQtyMantissa);
        assertEquals(dst.origQty().exponent(), out.origQtyExponent);
        assertEquals(dst.executedQty().mantissa(), out.executedQtyMantissa);
        assertEquals(dst.mergeLowPrice().mantissa(), out.mergeLowPriceMantissa);
        assertEquals((byte) 0, out.side, "src side=0(BUY)");
        assertEquals((byte) 1, out.type, "src type=1(LIMIT)");
        assertEquals((byte) 0, out.timeInForce, "src TIF=GTC→0");
        assertEquals((byte) 0, out.status, "src status=0");
        assertTrue(out.createTimeMillis > 0, "普通单时间字段保留");
        assertEquals(0L, out.visibleQtyMantissa, "src 无 visibleQty → 0");
    }

    private static OrderIn order(
            long id,
            long userId,
            String symbol,
            BigDecimal price,
            BigDecimal origQty,
            BigDecimal executedQty,
            BigDecimal mergeLowPrice,
            Byte side,
            Byte type,
            String timeInForce,
            Byte status,
            LocalDateTime createTime) {
        return new OrderIn(
                id,
                userId,
                symbol,
                price == null ? null : Decimal.fromBigDecimal(price),
                origQty == null ? null : Decimal.fromBigDecimal(origQty),
                executedQty == null ? null : Decimal.fromBigDecimal(executedQty),
                mergeLowPrice == null ? null : Decimal.fromBigDecimal(mergeLowPrice),
                side,
                type,
                timeInForce,
                status,
                createTime,
                null);
    }

    @Test
    void roundtripDepthSnapshot() {
        List<DepthItemVo> asks =
                List.of(
                        new DepthItemVo(Decimal.parse("70000.5"), Decimal.parse("1.25")),
                        new DepthItemVo(Decimal.parse("70001"), Decimal.parse("2")));
        List<DepthItemVo> bids =
                List.of(new DepthItemVo(Decimal.parse("69999"), Decimal.parse("0.5")));

        byte[] encoded = IoCodec.encodeDepthSnapshot("BTCUSDT", 1787310000000L, asks, bids);
        assertTrue(IoCodec.isSbe(encoded));

        DepthSnapshotMessage dst = IoCodec.decodeDepthSnapshot(encoded);
        assertEquals("BTCUSDT", dst.symbol());
        assertEquals(2, dst.asks().size());
        assertEquals(1, dst.bids().size());
        assertEquals(0, asks.get(0).price().compareTo(dst.asks().get(0).price()));
        assertEquals(0, asks.get(0).volume().compareTo(dst.asks().get(0).volume()));
        assertEquals(0, bids.get(0).price().compareTo(dst.bids().get(0).price()));
        assertEquals(0, bids.get(0).volume().compareTo(dst.bids().get(0).volume()));
    }

    @Test
    void roundtripCancelAck() {
        byte[] encoded = IoCodec.encodeCancelAck(123456789L);
        assertTrue(IoCodec.isSbe(encoded));
        assertEquals(123456789L, IoCodec.decodeCancelAck(encoded));
    }

    @Test
    void roundtripDltRecord() {
        byte[] encoded = IoCodec.encodeDltRecord("deserialization_failure", "4f435832");
        IoCodec.DltData d = IoCodec.decodeDltRecord(encoded);
        assertEquals("deserialization_failure", d.reason());
        assertEquals("4f435832", d.raw());
    }

    @Test
    void roundtripExceptionRecord() {
        byte[] encoded = IoCodec.encodeExceptionRecord(42L, "boom");
        IoCodec.ExceptionData e = IoCodec.decodeExceptionRecord(encoded);
        assertEquals(42L, e.sequence());
        assertEquals("boom", e.message());
    }

    @Test
    void roundtripOrderReject_withPrice() {
        Decimal remainingQty = Decimal.parse("0.5");
        Decimal price = Decimal.parse("70000.25");
        byte[] encoded =
                IoCodec.encodeOrderReject(
                        new IoCodec.OrderRejectData(
                                1001L,
                                7L,
                                "BTCUSDT",
                                "order_book_cap",
                                remainingQty.mantissa(),
                                remainingQty.exponent(),
                                true,
                                price.mantissa(),
                                price.exponent(),
                                1787310000000L));
        IoCodec.OrderRejectData d = IoCodec.decodeOrderReject(encoded);
        assertEquals(1001L, d.orderId());
        assertEquals(7L, d.userId());
        assertEquals("BTCUSDT", d.symbol());
        assertEquals("order_book_cap", d.reason());
        assertEquals(
                0,
                Decimal.parse("0.5")
                        .compareTo(Decimal.of(d.remainingQtyMantissa(), d.remainingQtyExponent())));
        assertEquals(
                0,
                Decimal.parse("70000.25")
                        .compareTo(Decimal.of(d.priceMantissa(), d.priceExponent())));
        assertTrue(d.pricePresent());
        assertEquals(1787310000000L, d.ts());
    }

    @Test
    void roundtripOrderReject_marketStopNoPrice() {
        byte[] encoded =
                IoCodec.encodeOrderReject(
                        new IoCodec.OrderRejectData(
                                1002L,
                                8L,
                                "BTCUSDT",
                                "order_book_cap",
                                Decimal.ONE.mantissa(),
                                Decimal.ONE.exponent(),
                                false,
                                0L,
                                0,
                                1787310001000L));
        IoCodec.OrderRejectData d = IoCodec.decodeOrderReject(encoded);
        assertEquals(1002L, d.orderId());
        assertEquals(
                0,
                Decimal.ONE.compareTo(
                        Decimal.of(d.remainingQtyMantissa(), d.remainingQtyExponent())));
        assertTrue(!d.pricePresent(), "无价格的条件单拒绝记录 pricePresent 应为 false");
    }

    @Test
    void batchOrderReject_批量帧可逐条解码() {
        List<IoCodec.OrderRejectData> rejects = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            Decimal qty = Decimal.ONE;
            Decimal price = i % 2 == 0 ? Decimal.parse("70000.25") : null;
            rejects.add(
                    new IoCodec.OrderRejectData(
                            10000L + i,
                            7L,
                            "BTCUSDT",
                            "order_book_cap",
                            qty.mantissa(),
                            qty.exponent(),
                            price != null,
                            price == null ? 0L : price.mantissa(),
                            price == null ? 0 : price.exponent(),
                            1787310000000L + i));
        }
        byte[] buf =
                new byte
                        [rejects.size()
                                * (IoCodec.FRAME_LENGTH_PREFIX
                                        + IoCodec.MAX_ORDER_REJECT_FRAME_LENGTH)];
        int len = IoCodec.encodeOrderRejectBatch(rejects, buf);
        byte[] payload = java.util.Arrays.copyOf(buf, len);

        assertEquals(300, IoCodec.countOrderRejects(payload));
        List<IoCodec.OrderRejectData> decoded = new ArrayList<>();
        IoCodec.forEachOrderReject(payload, decoded::add);
        assertEquals(300, decoded.size());
        for (int i = 0; i < 300; i++) {
            IoCodec.OrderRejectData d = decoded.get(i);
            assertEquals(10000L + i, d.orderId());
            assertEquals("BTCUSDT", d.symbol());
            assertEquals("order_book_cap", d.reason());
            if (i % 2 == 0) {
                assertEquals(
                        0,
                        com.ocx.math.Decimal.parse("70000.25")
                                .compareTo(Decimal.of(d.priceMantissa(), d.priceExponent())));
                assertTrue(d.pricePresent());
            } else {
                assertTrue(!d.pricePresent(), "奇数下标拒绝记录无价格");
            }
        }
    }

    /** 测试辅助：ExchangeTrade 数值字段按字符串构造 primitive（mantissa/exponent）。 */
    private static void setDecimal(
            java.util.function.BiConsumer<Long, Integer> setter, String value) {
        Decimal d = Decimal.parse(value);
        setter.accept(d.mantissa(), d.exponent());
    }
}
