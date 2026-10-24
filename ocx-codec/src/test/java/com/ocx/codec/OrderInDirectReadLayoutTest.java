package com.ocx.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.ocx.codec.domain.OrderIn;
import com.ocx.codec.domain.OrderInFields;
import com.ocx.math.Decimal;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * 直读偏移（{@link IoCodec#readOrderIn} 手写 F_*）与 SBE 生成解码器对拍。
 *
 * <p>防直读偏移与 schema 漂移：编码一条全字段非零帧，分别走「手写偏移直读」与
 * 「生成码解码」，所有字段必须一致。schema 只能尾部追加（见 ocx-io-schema.xml
 * 版本纪律注释）；若未来中段插字段导致偏移变化，本测试显式失败提醒同步 F_*。</p>
 */
class OrderInDirectReadLayoutTest {

    @Test
    void directReadMatchesGeneratedDecoderOnFullFrame() {
        LocalDateTime createTime = LocalDateTime.of(2026, 9, 2, 13, 30, 0);
        OrderIn src =
                new OrderIn(
                        123_456_789_012L,
                        990_001L,
                        "BTCUSDT",
                        Decimal.of(7_000_012_345_678L, -8),
                        Decimal.of(150_000_000L, -8),
                        Decimal.of(12_345_678L, -6),
                        Decimal.of(3_333_000_000_000L, -7),
                        (byte) 1,
                        (byte) 3,
                        "IOC",
                        (byte) 1,
                        createTime,
                        Decimal.of(88_000_000L, -7));

        byte[] encoded = IoCodec.encodeOrderIn(src);
        OrderInFields fields = new OrderInFields();
        IoCodec.readOrderIn(encoded, encoded.length, fields);

        // 生成码解码（唯一权威路径）
        OrderIn decoded = IoCodec.decodeOrderIn(encoded);

        assertEquals(decoded.id(), fields.id);
        assertEquals(decoded.userId(), fields.userId);
        assertEquals(decoded.price().mantissa(), fields.priceMantissa);
        assertEquals(decoded.price().exponent(), fields.priceExponent);
        assertEquals(decoded.origQty().mantissa(), fields.origQtyMantissa);
        assertEquals(decoded.origQty().exponent(), fields.origQtyExponent);
        assertEquals(decoded.executedQty().mantissa(), fields.executedQtyMantissa);
        assertEquals(decoded.executedQty().exponent(), fields.executedQtyExponent);
        assertEquals(decoded.mergeLowPrice().mantissa(), fields.mergeLowPriceMantissa);
        assertEquals(decoded.mergeLowPrice().exponent(), fields.mergeLowPriceExponent);
        assertEquals(decoded.side(), fields.side);
        assertEquals(decoded.type(), fields.type);
        assertEquals(IoCodec.TIF_IOC, fields.timeInForce);
        assertEquals("IOC", decoded.timeInForce());
        assertEquals(decoded.status(), fields.status);
        long expectedCreateMillis =
                createTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        assertEquals(expectedCreateMillis, fields.createTimeMillis);
        assertEquals(createTime, decoded.createTime());
        assertEquals(decoded.visibleQty().mantissa(), fields.visibleQtyMantissa);
        assertEquals(decoded.visibleQty().exponent(), fields.visibleQtyExponent);
    }
}
