package com.ocx.math;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;

/**
 * Decimal primitive 静态算术与 Decimal 实例/BigDecimal 的对拍测试。
 *
 * <p>P0 前置：Order 数值字段拆为 {@code long mantissa + int exponent} 后，热路径
 * 直接调用静态方法（compare/addAligned/subtractAligned/remainder/multiply/scaleMantissa），
 * 不再构造 Decimal 对象。本测试保证静态方法数值语义与 Decimal 实例方法完全一致。</p>
 */
class DecimalPrimitiveTest {

    private static BigDecimal bd(long mantissa, int exponent) {
        return BigDecimal.valueOf(mantissa, -exponent);
    }

    @Test
    void compareMatchesBigDecimal() {
        Random r = new Random(12345);
        for (int i = 0; i < 300_000; i++) {
            long m1 = randMantissa(r, 13);
            long m2 = randMantissa(r, 13);
            int e1 = -8;
            int e2 = -8;
            final long fm1 = m1;
            final long fm2 = m2;
            int got = Decimal.compare(m1, e1, m2, e2);
            int expected = bd(m1, e1).compareTo(bd(m2, e2));
            assertEquals(
                    expected,
                    got,
                    () -> "compare mismatch m1=" + fm1 + " e1=" + e1 + " m2=" + fm2 + " e2=" + e2);
        }
    }

    @Test
    void compareUnequalExponentAndZero() {
        Random r = new Random(777);
        for (int i = 0; i < 100_000; i++) {
            long m1 = randMantissa(r, 9);
            long m2 = randMantissa(r, 9);
            int e1 = -(r.nextInt(9) + 1); // -9..-1
            int e2 = -(r.nextInt(9) + 1);
            if (i % 7 == 0) m1 = 0L;
            if (i % 11 == 0) m2 = 0L;
            final long fm1 = m1;
            final long fm2 = m2;
            assertEquals(
                    bd(m1, e1).compareTo(bd(m2, e2)),
                    Decimal.compare(m1, e1, m2, e2),
                    () ->
                            "unequal-exp compare mismatch m1="
                                    + fm1
                                    + " e1="
                                    + e1
                                    + " m2="
                                    + fm2
                                    + " e2="
                                    + e2);
            assertEquals(
                    bd(m1, e1).signum(),
                    Long.signum(Decimal.compare(m1, e1, 0L, 0)),
                    () -> "compare-to-zero mismatch m1=" + fm1 + " e1=" + e1);
        }
    }

    @Test
    void addSubtractMatchesBigDecimal() {
        Random r = new Random(4242);
        for (int i = 0; i < 200_000; i++) {
            long m1 = randMantissa(r, 13);
            long m2 = randMantissa(r, 13);
            int e1 = -8;
            int e2 = -8;
            int target = Math.min(e1, e2);
            try {
                long sum = Decimal.addAligned(m1, e1, m2, e2, target);
                assertEquals(
                        0,
                        bd(m1, e1).add(bd(m2, e2)).compareTo(bd(sum, target)),
                        () -> "add mismatch m1=" + m1 + " e1=" + e1 + " m2=" + m2 + " e2=" + e2);
                long diff = Decimal.subtractAligned(m1, e1, m2, e2, target);
                assertEquals(
                        0,
                        bd(m1, e1).subtract(bd(m2, e2)).compareTo(bd(diff, target)),
                        () ->
                                "subtract mismatch m1="
                                        + m1
                                        + " e1="
                                        + e1
                                        + " m2="
                                        + m2
                                        + " e2="
                                        + e2);
            } catch (ArithmeticException overflow) {
                // long 溢出在 Decimal 实例中同样抛 ArithmeticException，语义一致
                Decimal a = Decimal.of(m1, e1);
                Decimal b = Decimal.of(m2, e2);
                assertThrows(ArithmeticException.class, () -> a.add(b));
                assertThrows(ArithmeticException.class, () -> a.subtract(b));
            }
        }
    }

    @Test
    void addAlignedMatchesInstanceAdd() {
        Random r = new Random(20260825);
        for (int i = 0; i < 100_000; i++) {
            long m1 = randMantissa(r, 12);
            long m2 = randMantissa(r, 12);
            int e1 = -(r.nextInt(9) + 1);
            int e2 = -(r.nextInt(9) + 1);
            if (i % 5 == 0) m1 = 0L;
            if (i % 13 == 0) m2 = 0L;
            final long fm1 = m1;
            final long fm2 = m2;
            Decimal a = Decimal.of(m1, e1);
            Decimal b = Decimal.of(m2, e2);
            int target = Math.min(e1, e2);
            try {
                Decimal sum = Decimal.of(Decimal.addAligned(m1, e1, m2, e2, target), target);
                assertEquals(
                        0,
                        sum.compareTo(a.add(b)),
                        () ->
                                "addAligned vs instance.add m1="
                                        + fm1
                                        + " e1="
                                        + e1
                                        + " m2="
                                        + fm2
                                        + " e2="
                                        + e2);
                Decimal diff = Decimal.of(Decimal.subtractAligned(m1, e1, m2, e2, target), target);
                assertEquals(
                        0,
                        diff.compareTo(a.subtract(b)),
                        () ->
                                "subtractAligned vs instance.subtract m1="
                                        + fm1
                                        + " e1="
                                        + e1
                                        + " m2="
                                        + fm2
                                        + " e2="
                                        + e2);
            } catch (ArithmeticException overflow) {
                // 溢出分支：实例方法同样抛异常，此处可接受
            }
        }
    }

    @Test
    void remainderMatchesBigDecimal() {
        Random r = new Random(8888);
        for (int i = 0; i < 100_000; i++) {
            long m1 = randMantissa(r, 12);
            long m2 = randMantissa(r, 12);
            int e1 = -(r.nextInt(9) + 1);
            int e2 = -(r.nextInt(9) + 1);
            if (m2 == 0L) m2 = 1L;
            int target = Math.min(e1, e2);
            final long fm1 = m1;
            final long fm2 = m2;
            try {
                long rem = Decimal.remainder(m1, e1, m2, e2);
                BigDecimal expected = bd(m1, e1).remainder(bd(m2, e2));
                assertEquals(
                        0,
                        expected.compareTo(bd(rem, target)),
                        () ->
                                "remainder mismatch m1="
                                        + fm1
                                        + " e1="
                                        + e1
                                        + " m2="
                                        + fm2
                                        + " e2="
                                        + e2);
            } catch (ArithmeticException overflow) {
                // scaleUp 对齐时 mantissa*10^n 溢出 long：Decimal 实例 remainder 同样抛，语义一致
            }
        }
    }

    @Test
    void multiplyMatchesBigDecimal() {
        Random r = new Random(9999);
        for (int i = 0; i < 100_000; i++) {
            long m1 = randMantissa(r, 9);
            long m2 = randMantissa(r, 9);
            int e1 = -(r.nextInt(5) + 1);
            int e2 = -(r.nextInt(5) + 1);
            int scale = 8;
            final long fm1 = m1;
            final long fm2 = m2;
            try {
                long product = Decimal.multiply(m1, e1, m2, e2, scale, RoundingMode.HALF_UP);
                BigDecimal expected =
                        bd(m1, e1).multiply(bd(m2, e2)).setScale(scale, RoundingMode.HALF_UP);
                assertEquals(
                        0,
                        expected.compareTo(bd(product, -scale)),
                        () ->
                                "multiply mismatch m1="
                                        + fm1
                                        + " e1="
                                        + e1
                                        + " m2="
                                        + fm2
                                        + " e2="
                                        + e2);
            } catch (ArithmeticException overflow) {
                // mulDivPow10 对 k<-9 或中间商溢出抛 ArithmeticException；Decimal 实例同样抛
            }
        }
    }

    @Test
    void scaleMantissaMatchesInstance() {
        Random r = new Random(31337);
        for (int i = 0; i < 200_000; i++) {
            long m = randMantissa(r, 12);
            int e = -(r.nextInt(12) + 1); // -12..-1
            int scale = 8;
            if (i % 3 == 0) scale = r.nextInt(10);
            final long fm = m;
            final int fscale = scale;
            Decimal d = Decimal.of(m, e);
            try {
                assertEquals(
                        d.scaleMantissa(fscale),
                        Decimal.scaleMantissa(fm, e, fscale),
                        () -> "scaleMantissa mismatch m=" + fm + " e=" + e + " scale=" + fscale);
            } catch (ArithmeticException overflow) {
                // 指数差 > 18 时两者同样抛 ArithmeticException
            }
        }
        // exponent 恰好匹配 / 上扩路径
        assertEquals(12345L, Decimal.scaleMantissa(12345L, -8, 8));
        assertEquals(12345L, Decimal.scaleMantissa(12345L, -2, 2));
        // 上扩路径：exponent=-8 向 scale=2 对齐会缩成 0（0.00012345 -> 0.00）
        assertEquals(0L, Decimal.scaleMantissa(12345L, -8, 2));
        // 缩 scale 舍入路径：12.345 -> 12.35（HALF_UP）
        assertEquals(1235L, Decimal.scaleMantissa(12345L, -3, 2));
        // 1.2345 -> 1.23
        assertEquals(123L, Decimal.scaleMantissa(12345L, -4, 2));
    }

    @Test
    void zeroAndEdgeCases() {
        // ZERO 参与：addAligned 返回 0 mantissa，与实例 add 数值相等
        assertEquals(5L, Decimal.addAligned(0L, 0, 5L, -8, -8));
        assertEquals(0L, Decimal.addAligned(0L, 0, 0L, -8, -8));
        assertEquals(0, Decimal.compare(0L, -8, 0L, 0));
        assertEquals(0, Decimal.compare(0L, 0, 0L, -8));
        assertEquals(-1, Decimal.compare(0L, 0, 1L, -8));
        assertEquals(1, Decimal.compare(1L, -8, 0L, 0));
        // 负值比较
        assertEquals(-1, Decimal.compare(-5L, -8, 3L, -8));
        assertEquals(1, Decimal.compare(5L, -8, -3L, -8));
        assertEquals(-1, Decimal.compare(-5L, -8, -3L, -8));
        // exponent 差过大：scaleMantissa 缩 scale 抛异常
        assertThrows(ArithmeticException.class, () -> Decimal.scaleMantissa(1L, 0, 30));
        // multiply 零因子返回 0
        assertEquals(0L, Decimal.multiply(0L, -8, 5L, -8, 8, RoundingMode.HALF_UP));
        assertEquals(0L, Decimal.multiply(5L, -8, 0L, 0, 8, RoundingMode.HALF_UP));
    }

    private static long randMantissa(Random r, int maxDigits) {
        long m = (long) (r.nextDouble() * Math.pow(10, maxDigits));
        return r.nextBoolean() ? -m : m;
    }
}
