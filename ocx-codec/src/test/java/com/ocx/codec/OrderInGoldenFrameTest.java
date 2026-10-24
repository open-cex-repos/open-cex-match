package com.ocx.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.ocx.codec.domain.OrderIn;
import com.ocx.codec.sbe.OrderInDecoder;
import com.ocx.math.Decimal;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * SBE OrderIn 黄金帧双端对拍（schema 契约的唯一字节真值）。
 *
 * <p>fixture（{@code ocx-codec/src/test/resources/sbe-golden/orderin-*.hex}）由
 * 本测试（mvn test）与 {@code ocx-stress-producer/src/order_in.rs} 的 cargo 单测
 * 共同断言：Java 生成码编码器与 Rust 手写编码器必须产出逐字节相同的帧。任一
 * 侧改动（含 schema version/blockLength/字段偏移/字节序）在双端测试都会显式失败，
 * 任何 CI（mvn test + cargo test）即可自动对拍。</p>
 */
class OrderInGoldenFrameTest {

    @Test
    void limitBuyMatchesFixture() throws IOException {
        OrderIn order =
                new OrderIn(
                        7L,
                        99L,
                        "BTCUSDT",
                        Decimal.of(7_000_012_345_678L, -8),
                        Decimal.of(150_000_000L, -8),
                        null,
                        null,
                        (byte) 0,
                        (byte) 1,
                        null,
                        (byte) 0,
                        null,
                        null);
        assertGolden("orderin-limit-buy.hex", order);
    }

    @Test
    void icebergMatchesFixture() throws IOException {
        OrderIn order =
                new OrderIn(
                        8L,
                        99L,
                        "BTCUSDT",
                        Decimal.of(7_000_012_345_678L, -8),
                        Decimal.of(150_000_000L, -8),
                        null,
                        null,
                        (byte) 0,
                        (byte) 1,
                        null,
                        (byte) 0,
                        null,
                        Decimal.of(60_000_000L, -8));
        assertGolden("orderin-iceberg.hex", order);
    }

    @Test
    void marketSellMatchesFixture() throws IOException {
        OrderIn order =
                new OrderIn(
                        9L,
                        99L,
                        "BTCUSDT",
                        null,
                        Decimal.of(150_000_000L, -8),
                        null,
                        null,
                        (byte) 1,
                        (byte) 2,
                        null,
                        (byte) 0,
                        null,
                        null);
        assertGolden("orderin-market-sell.hex", order);
    }

    private static void assertGolden(String fixtureName, OrderIn order) throws IOException {
        byte[] fixture = readFixture(fixtureName);
        byte[] encoded = IoCodec.encodeOrderIn(order);

        // 帧头不变量：魔数 + SBE header 四元组（blockLength/templateId/schemaId/version）
        assertEquals("OCX2", new String(fixture, 0, 4, StandardCharsets.US_ASCII));
        assertHeaderField(fixture, 4, OrderInDecoder.BLOCK_LENGTH);
        assertHeaderField(fixture, 6, OrderInDecoder.TEMPLATE_ID);
        assertHeaderField(fixture, 8, OrderInDecoder.SCHEMA_ID);
        assertHeaderField(fixture, 10, OrderInDecoder.SCHEMA_VERSION);

        assertEquals(hex(fixture), hex(encoded), "Java 编码帧必须与共享 fixture 逐字节一致");
        assertEquals(fixture.length, encoded.length);
    }

    private static void assertHeaderField(byte[] frame, int offset, int expected) {
        int actual = (frame[offset] & 0xFF) | ((frame[offset + 1] & 0xFF) << 8);
        assertEquals(expected, actual, "SBE header offset=" + offset);
    }

    private static byte[] readFixture(String name) throws IOException {
        try (InputStream in =
                OrderInGoldenFrameTest.class.getResourceAsStream("/sbe-golden/" + name)) {
            if (in == null) {
                throw new IOException("fixture 缺失: /sbe-golden/" + name);
            }
            String line = new String(in.readAllBytes(), StandardCharsets.US_ASCII).trim();
            byte[] bytes = new byte[line.length() / 2];
            for (int i = 0; i < bytes.length; i++) {
                bytes[i] = (byte) Integer.parseInt(line.substring(i * 2, i * 2 + 2), 16);
            }
            return bytes;
        }
    }

    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xFF));
        }
        return sb.toString();
    }
}
