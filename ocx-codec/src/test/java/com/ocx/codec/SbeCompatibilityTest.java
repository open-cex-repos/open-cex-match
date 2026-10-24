package com.ocx.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.ocx.codec.domain.OrderIn;
import com.ocx.codec.sbe.DecimalValueEncoder;
import com.ocx.codec.sbe.MessageHeaderDecoder;
import com.ocx.codec.sbe.MessageHeaderEncoder;
import com.ocx.codec.sbe.OrderInDecoder;
import com.ocx.codec.sbe.OrderInEncoder;
import com.ocx.math.Decimal;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * SBE wire 契约与前向兼容测试。
 *
 * <p>钉死撮合协议在 wire 上的关键不变量（schemaId/templateId/version、decimalValue
 * 布局），并验证 SBE 的版本演进语义：未来 schema 在消息尾部新增字段（blockLength
 * 增长 + version 提升）时，旧版本消费者仍能完整读取旧字段——防止 schema 演进
 * 破坏撮合/外围/历史 WAL 的互操作。</p>
 */
class SbeCompatibilityTest {

    /** 业务 IO schema（ocx-io-schema.xml，id=102）。 */
    private static final int IO_SCHEMA_ID = 102;

    /** WAL schema（ocx-wal-schema.xml，id=101）。 */
    private static final int WAL_SCHEMA_ID = 101;

    private static final int ORDER_IN_TEMPLATE_ID = 1;

    @Test
    void wireContractHeaderAndDecimalLayout() {
        // decimalValue = mantissa(int64) × 10^exponent(int8) → 固定 9 字节
        assertEquals(
                9,
                DecimalValueEncoder.ENCODED_LENGTH,
                "decimalValue 必须是 mantissa(int64)+exponent(int8) 的 9 字节布局");
        assertEquals(IO_SCHEMA_ID, DecimalValueEncoder.SCHEMA_ID);
        // io schema 历经订单号语义修订/冰山/mergeLowPrice 演进，当前 version=1
        assertEquals(1, DecimalValueEncoder.SCHEMA_VERSION);

        byte[] buf = new byte[256];
        UnsafeBuffer b = new UnsafeBuffer(buf);
        MessageHeaderEncoder header = new MessageHeaderEncoder();
        OrderInEncoder enc = new OrderInEncoder();
        enc.wrapAndApplyHeader(b, 0, header);
        enc.id(42L).userId(7L);

        MessageHeaderDecoder dec = new MessageHeaderDecoder();
        dec.wrap(b, 0);
        assertEquals(IO_SCHEMA_ID, dec.schemaId(), "OrderIn 必须属于 io schema(102)");
        assertEquals(ORDER_IN_TEMPLATE_ID, dec.templateId());
        assertEquals(1, dec.version());
        assertEquals(OrderInEncoder.BLOCK_LENGTH, dec.blockLength());
    }

    @Test
    void forwardCompatibilityNewMessageReadByOldDecoder() {
        // 用 IoCodec 编码一条完整 OrderIn（含 varString symbol）
        OrderIn src =
                new OrderIn(
                        123456L,
                        990001L,
                        "BTCUSDT",
                        Decimal.parse("70000.5"),
                        Decimal.parse("1.25"),
                        null,
                        null,
                        (byte) 0,
                        (byte) 1,
                        null,
                        (byte) 0,
                        null,
                        null);
        byte[] frame = IoCodec.encodeOrderIn(src);

        UnsafeBuffer b = new UnsafeBuffer(frame);
        int sbeOffset = IoCodec.MAGIC_LEN;
        MessageHeaderEncoder header = new MessageHeaderEncoder();
        header.wrap(b, sbeOffset);
        int payloadLen = frame.length;

        // 模拟未来 schema v2：version 提升、blockLength 增长（尾部新增 4 字节字段）
        MessageHeaderDecoder cur = new MessageHeaderDecoder();
        cur.wrap(b, sbeOffset);
        int futureBlockLength = cur.blockLength() + 4;
        header.version(2);
        header.blockLength(futureBlockLength);
        // 扩容缓冲追加新字段字节
        byte[] extended = new byte[payloadLen + 4];
        System.arraycopy(frame, 0, extended, 0, payloadLen);
        UnsafeBuffer eb = new UnsafeBuffer(extended);
        eb.putInt(payloadLen, 0xCAFEBABE); // 新字段（旧消费者应忽略）

        // 旧版本消费者（当前 decoder）读未来版本消息
        MessageHeaderDecoder dec = new MessageHeaderDecoder();
        dec.wrap(eb, sbeOffset);
        assertEquals(2, dec.version());
        OrderInDecoder d = new OrderInDecoder();
        d.wrap(
                eb,
                sbeOffset + MessageHeaderDecoder.ENCODED_LENGTH,
                dec.blockLength(),
                dec.version());

        assertEquals(123456L, d.id());
        assertEquals(990001L, d.userId());
        assertEquals(7, d.symbol().length(), "BTCUSDT 定长 symbol 解码 trim 零填充后长度");
        // 数值等价：mantissa × 10^exponent == 70000.5（不钉死 scale 表示）
        double gotPrice = d.price().mantissa() * Math.pow(10, d.price().exponent());
        assertEquals(70000.5, gotPrice, 1e-6);
        // 尾部新字段不干扰旧字段读取
        assertEquals(0xCAFEBABE, eb.getInt(payloadLen));
    }

    @Test
    void forwardCompatibilityOrderCreateWal() {
        // WAL OrderCreate 也走同一 SBE 版本语义（schema id=101，独立演进）
        assertEquals(WAL_SCHEMA_ID, com.ocx.codec.sbe.wal.OrderCreateEncoder.SCHEMA_ID);
        assertEquals(9, com.ocx.codec.sbe.wal.DecimalValueEncoder.ENCODED_LENGTH);
        assertEquals(1, com.ocx.codec.sbe.wal.OrderCreateEncoder.SCHEMA_VERSION);
    }
}
