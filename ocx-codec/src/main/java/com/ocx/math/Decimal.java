package com.ocx.math;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * 撮合引擎内部定点小数。
 *
 * <p>数值表示为 {@code mantissa * 10^exponent}，其中 {@code mantissa} 是 {@code long}，
 * {@code exponent} 是 {@code int}。该表示与 SBE DecimalValue 的
 * {@code mantissa(int64) x 10^exponent(int8)} 以及 WAL 编解码完全一致。</p>
 *
 * <p>撮合热路径中的价格/数量/金额都会在边界处归一化到 symbol 的固定精度（例如
 * {@code coinScale=8}，即 {@code exponent=-8}），因此 {@code add/subtract/compare/remainder}
 * 走纯 {@code long} 快速路径；乘法使用 128 位中间结果避免 {@code long * long} 溢出。</p>
 *
 * <p>设计目标：撮合核心不再依赖 {@link BigDecimal}，数值运算集中在本类，不允许散落到
 * OrderBooks/MatchService 等业务类。边界转换（{@link #fromBigDecimal} / {@link #toBigDecimal}）
 * 只发生在协议/JPA 边界。</p>
 */
public final class Decimal implements Comparable<Decimal> {

    public static final Decimal ZERO = new Decimal(0L, 0);
    public static final Decimal ONE = new Decimal(1L, 0);

    private static final long MASK32 = 0xffff_ffffL;

    /**
     * {@link #of(long, int)} 的复用缓存：tick/step 量化下价格、数量取值高度重复，
     * 消费侧解码、WAL 恢复、盘口 best price 等路径直接复用不可变对象，消除重复分配。
     *
     * <p>槽位固定（2^18）内存有界：实测 40K×600s 压测单 symbol 价格档位约 4 万个
     * （0.01 tick × ±200 区间），槽位需覆盖全部档位才有意义；qty 为连续随机值时
     * 按 (mantissa, exponent) 均匀散列到各槽，平均每个价格槽在两次出现之间被
     * qty 覆盖的概率极低（实测价格命中率 ~90%+）。并发下仅存在"多建一个等价
     * 对象"的良性竞争。</p>
     */
    private static final int DECIMAL_CACHE_SLOTS = 1 << 18;

    private static final AtomicReferenceArray<Decimal> DECIMAL_CACHE =
            new AtomicReferenceArray<>(DECIMAL_CACHE_SLOTS);
    private static final long[] POW10 = {
        1L,
        10L,
        100L,
        1_000L,
        10_000L,
        100_000L,
        1_000_000L,
        10_000_000L,
        100_000_000L,
        1_000_000_000L,
        10_000_000_000L,
        100_000_000_000L,
        1_000_000_000_000L,
        10_000_000_000_000L,
        100_000_000_000_000L,
        1_000_000_000_000_000L,
        10_000_000_000_000_000L,
        100_000_000_000_000_000L,
        1_000_000_000_000_000_000L
    };

    private final long mantissa;
    private final int exponent;

    private Decimal(long mantissa, int exponent) {
        this.mantissa = mantissa;
        this.exponent = exponent;
    }

    /** 直接以 mantissa/exponent 构造，不做归一化。value = mantissa * 10^exponent。 */
    public static Decimal of(long mantissa, int exponent) {
        if (mantissa == 0L) {
            return ZERO;
        }
        int slot = decimalCacheSlot(mantissa, exponent);
        Decimal cached = DECIMAL_CACHE.get(slot);
        if (cached != null && cached.mantissa == mantissa && cached.exponent == exponent) {
            return cached;
        }
        Decimal d = new Decimal(mantissa, exponent);
        DECIMAL_CACHE.set(slot, d);
        return d;
    }

    private static int decimalCacheSlot(long mantissa, int exponent) {
        long h = mantissa * 0x9E3779B97F4A7C15L + exponent;
        h ^= h >>> 33;
        h *= 0xFF51AFD7ED558CCDL;
        h ^= h >>> 33;
        return (int) (h & (DECIMAL_CACHE_SLOTS - 1));
    }

    public static Decimal fromLong(long value) {
        return value == 0L ? ZERO : new Decimal(value, 0);
    }

    /** 边界转换：保留 BigDecimal 自身精度。 */
    public static Decimal fromBigDecimal(BigDecimal value) {
        if (value == null) {
            return null;
        }
        if (value.compareTo(BigDecimal.ZERO) == 0) {
            return ZERO;
        }
        return new Decimal(value.unscaledValue().longValueExact(), -value.scale());
    }

    /** 边界转换：归一化到指定 scale（十进制小数位数），value = mantissa * 10^-scale。 */
    public static Decimal fromBigDecimal(BigDecimal value, int scale) {
        if (value == null) {
            return null;
        }
        BigDecimal normalized = value.setScale(scale, RoundingMode.HALF_UP);
        return normalized.compareTo(BigDecimal.ZERO) == 0
                ? ZERO
                : new Decimal(normalized.unscaledValue().longValueExact(), -scale);
    }

    /** 边界转换：恢复 BigDecimal（仅用于跨模块协议/JPA 边界，禁止进入撮合核心运算）。 */
    @JsonValue
    public BigDecimal toBigDecimal() {
        return BigDecimal.valueOf(mantissa, -exponent);
    }

    /** Jackson 反序列化入口：JSON 数值 → Decimal。 */
    @JsonCreator
    public static Decimal fromJson(BigDecimal value) {
        return fromBigDecimal(value);
    }

    /** 从普通十进制字符串解析（无指数形式）。仅恢复/测试等冷路径使用。 */
    public static Decimal parse(String text) {
        if (text == null) {
            return null;
        }
        String s = text.trim();
        if (s.isEmpty()) {
            throw new NumberFormatException("empty decimal: " + text);
        }
        boolean negative = s.charAt(0) == '-';
        if (negative || s.charAt(0) == '+') {
            s = s.substring(1);
        }
        int dot = s.indexOf('.');
        String intPart;
        String fracPart;
        if (dot < 0) {
            intPart = s;
            fracPart = "";
        } else {
            intPart = s.substring(0, dot);
            fracPart = s.substring(dot + 1);
        }
        if (intPart.isEmpty() && fracPart.isEmpty()) {
            throw new NumberFormatException("empty decimal: " + text);
        }
        String digits = intPart + fracPart;
        if (digits.isEmpty() || !digits.chars().allMatch(Character::isDigit)) {
            throw new NumberFormatException("invalid decimal: " + text);
        }
        long unscaled = Long.parseLong(digits);
        if (negative) {
            unscaled = -unscaled;
        }
        if (unscaled == 0L) {
            return ZERO;
        }
        return new Decimal(unscaled, -fracPart.length());
    }

    public long mantissa() {
        return mantissa;
    }

    /** 返回归一化到指定 scale 的原始 long mantissa，供订单簿做 primitive 价格键。 */
    public long scaleMantissa(int scale) {
        return scaleMantissa(mantissa, exponent, scale);
    }

    /**
     * 将 {@code (mantissa, exponent)} 归一化到指定 scale（decimal 位数）的 long mantissa。
     *
     * <p>语义与 {@link #scaleMantissa(int)} 一致：exponent 已匹配直接返回；指数更大时
     * 上扩（乘 10 的幂），更小时 HALF_UP 舍入。供持有 primitive 字段的调用方在热路径使用。</p>
     */
    public static long scaleMantissa(long mantissa, int exponent, int scale) {
        int targetExponent = -scale;
        if (exponent == targetExponent) {
            return mantissa;
        }
        if (exponent > targetExponent) {
            return scaleUp(mantissa, exponent, targetExponent);
        }
        int p = targetExponent - exponent;
        if (p > 18) {
            throw new ArithmeticException("Decimal scale down overflow");
        }
        return roundDiv(mantissa, POW10[p], RoundingMode.HALF_UP);
    }

    public int exponent() {
        return exponent;
    }

    public int scale() {
        return -exponent;
    }

    public boolean isZero() {
        return mantissa == 0L;
    }

    public boolean isPositive() {
        return mantissa > 0L;
    }

    public boolean isNegative() {
        return mantissa < 0L;
    }

    public int signum() {
        return Long.signum(mantissa);
    }

    @Override
    public int compareTo(Decimal other) {
        if (other == null) {
            return 1;
        }
        return compare(mantissa, exponent, other.mantissa, other.exponent);
    }

    /**
     * 比较两个 {@code (mantissa, exponent)} 表示的数值，语义与 {@link #compareTo(Decimal)} 一致。
     *
     * <p>撮合热路径使用：调用方（Order 等）持有 primitive 字段时直接比较，避免构造
     * {@link Decimal} 对象与缓存查找。</p>
     */
    public static int compare(long m1, int e1, long m2, int e2) {
        if (m1 == 0L || m2 == 0L) {
            if (m1 == 0L && m2 == 0L) {
                return 0;
            }
            return m1 == 0L ? (m2 > 0L ? -1 : 1) : (m1 > 0L ? 1 : -1);
        }
        if ((m1 < 0L) != (m2 < 0L)) {
            return m1 < 0L ? -1 : 1;
        }
        int sign = m1 < 0L ? -1 : 1;
        if (e1 == e2) {
            return sign * Long.compare(abs(m1), abs(m2));
        }
        return sign * compareMagnitude(abs(m1), e1, abs(m2), e2);
    }

    public Decimal add(Decimal other) {
        if (other == null || other.isZero()) {
            return this;
        }
        if (isZero()) {
            return other;
        }
        int targetExponent = Math.min(exponent, other.exponent);
        return new Decimal(
                addAligned(mantissa, exponent, other.mantissa, other.exponent, targetExponent),
                targetExponent);
    }

    /**
     * 对齐到 {@code targetExponent} 后相加，返回该 exponent 下的 mantissa。
     *
     * <p>数值语义与 {@link #add(Decimal)} 一致（结果 exponent 即 {@code targetExponent}）；
     * 调用方需自行保证 {@code targetExponent <= min(e1, e2)}（上取对齐方向），
     * 通常传 {@code Math.min(e1, e2)}。</p>
     */
    public static long addAligned(long m1, int e1, long m2, int e2, int targetExponent) {
        long a = e1 == targetExponent ? m1 : scaleUp(m1, e1, targetExponent);
        long b = e2 == targetExponent ? m2 : scaleUp(m2, e2, targetExponent);
        return Math.addExact(a, b);
    }

    public Decimal subtract(Decimal other) {
        if (other == null || other.isZero()) {
            return this;
        }
        if (isZero()) {
            return other.negate();
        }
        int targetExponent = Math.min(exponent, other.exponent);
        return new Decimal(
                subtractAligned(mantissa, exponent, other.mantissa, other.exponent, targetExponent),
                targetExponent);
    }

    /** 对齐到 {@code targetExponent} 后相减，返回该 exponent 下的 mantissa。语义同 {@link #subtract(Decimal)}。 */
    public static long subtractAligned(long m1, int e1, long m2, int e2, int targetExponent) {
        long a = e1 == targetExponent ? m1 : scaleUp(m1, e1, targetExponent);
        long b = e2 == targetExponent ? m2 : scaleUp(m2, e2, targetExponent);
        return Math.subtractExact(a, b);
    }

    public Decimal negate() {
        return mantissa == 0L ? ZERO : new Decimal(-mantissa, exponent);
    }

    public Decimal min(Decimal other) {
        return compareTo(other) <= 0 ? this : other;
    }

    public Decimal max(Decimal other) {
        return compareTo(other) >= 0 ? this : other;
    }

    public Decimal remainder(Decimal divisor) {
        if (divisor == null || divisor.isZero()) {
            throw new ArithmeticException("division by zero");
        }
        int targetExponent = Math.min(exponent, divisor.exponent);
        return new Decimal(
                remainder(mantissa, exponent, divisor.mantissa, divisor.exponent), targetExponent);
    }

    /**
     * 求余：两数对齐到 {@code min(e1, e2)} 后取模，返回该 exponent 下的余数 mantissa。
     * 语义与 {@link #remainder(Decimal)} 一致。
     */
    public static long remainder(long m1, int e1, long m2, int e2) {
        int targetExponent = Math.min(e1, e2);
        long a = e1 == targetExponent ? m1 : scaleUp(m1, e1, targetExponent);
        long b = e2 == targetExponent ? m2 : scaleUp(m2, e2, targetExponent);
        return a % b;
    }

    /**
     * 乘法并舍入到指定 scale。
     *
     * <p>等价于 {@code this.multiply(other).setScale(scale, mode)}，但使用 128 位中间结果，
     * 避免 {@code long} 乘积溢出，且不产生对象分配（快速路径）。</p>
     */
    public Decimal multiply(Decimal other, int scale, RoundingMode mode) {
        if (other == null || isZero() || other.isZero()) {
            return ZERO;
        }
        return new Decimal(
                multiply(mantissa, exponent, other.mantissa, other.exponent, scale, mode), -scale);
    }

    /**
     * 乘法并舍入到指定 scale（结果 exponent = {@code -scale}），返回该 exponent 下的 mantissa。
     *
     * <p>使用 128 位中间结果避免 {@code long * long} 溢出；语义与
     * {@link #multiply(Decimal, int, RoundingMode)} 一致（因子为零时返回 0）。</p>
     */
    public static long multiply(long m1, int e1, long m2, int e2, int scale, RoundingMode mode) {
        if (m1 == 0L || m2 == 0L) {
            return 0L;
        }
        boolean negative = (m1 < 0L) ^ (m2 < 0L);
        long a = abs(m1);
        long b = abs(m2);
        int k = e1 + e2 + scale;
        long result;
        if (k < 0) {
            result = mulDivPow10(a, b, -k, mode);
        } else {
            result = mulPow10Exact(Math.multiplyExact(a, b), k);
        }
        return negative ? -result : result;
    }

    /** 除以一个小整数并舍入到指定 scale。当前仅用于 bestBid/bestAsk 中价计算。 */
    public Decimal divideByLong(long divisor, int scale, RoundingMode mode) {
        if (divisor == 0L) {
            throw new ArithmeticException("division by zero");
        }
        if (isZero()) {
            return ZERO;
        }
        int k = exponent + scale;
        if (k == 0) {
            long q = roundDiv(mantissa, divisor, mode);
            return new Decimal(q, -scale);
        }
        throw new ArithmeticException("unsupported Decimal divideByLong scale alignment");
    }

    public String toPlainString() {
        if (isZero()) {
            return exponent >= 0 ? "0" : "0." + "0".repeat(-exponent);
        }
        String digits = Long.toString(Math.abs(mantissa));
        StringBuilder sb = new StringBuilder(digits.length() + 4);
        if (mantissa < 0L) {
            sb.append('-');
        }
        if (exponent >= 0) {
            sb.append(digits);
            for (int i = 0; i < exponent; i++) {
                sb.append('0');
            }
            return sb.toString();
        }
        int scale = -exponent;
        if (digits.length() <= scale) {
            sb.append("0.");
            for (int i = digits.length(); i < scale; i++) {
                sb.append('0');
            }
            sb.append(digits);
        } else {
            int split = digits.length() - scale;
            sb.append(digits, 0, split).append('.').append(digits, split, digits.length());
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Decimal other)) {
            return false;
        }
        if (mantissa == other.mantissa) {
            return exponent == other.exponent;
        }
        return compareTo(other) == 0;
    }

    @Override
    public int hashCode() {
        // 归一化到最简 mantissa，避免 1.0 与 1.00 因 exponent 不同导致 hashCode 不一致。
        if (mantissa == 0L) {
            return 0;
        }
        long m = mantissa;
        int e = exponent;
        while ((m % 10L) == 0L) {
            m /= 10L;
            e++;
        }
        return Long.hashCode(m) * 31 + Integer.hashCode(e);
    }

    @Override
    public String toString() {
        return toPlainString();
    }

    private static int compareMagnitude(long m1, int e1, long m2, int e2) {
        if (e1 == e2) {
            return Long.compare(m1, m2);
        }
        int d = e1 - e2;
        if (d > 0) {
            if (d > 18) {
                return 1;
            }
            long p = POW10[d];
            if (m1 > Long.MAX_VALUE / p) {
                return 1;
            }
            return Long.compare(m1 * p, m2);
        }
        int n = -d;
        if (n > 18) {
            return -1;
        }
        long p = POW10[n];
        if (m2 > Long.MAX_VALUE / p) {
            return -1;
        }
        return Long.compare(m1, m2 * p);
    }

    private static long scaleUp(long mantissa, int fromExponent, int toExponent) {
        int n = fromExponent - toExponent;
        if (n < 0 || n > 18) {
            throw new ArithmeticException("Decimal exponent alignment overflow");
        }
        long p = POW10[n];
        if (mantissa > Long.MAX_VALUE / p || mantissa < Long.MIN_VALUE / p) {
            throw new ArithmeticException("Decimal exponent alignment overflow");
        }
        return mantissa * p;
    }

    private static long mulPow10Exact(long value, int n) {
        if (n < 0 || n > 18) {
            throw new ArithmeticException("Decimal multiply overflow");
        }
        long p = POW10[n];
        if (value > Long.MAX_VALUE / p || value < Long.MIN_VALUE / p) {
            throw new ArithmeticException("Decimal multiply overflow");
        }
        return value * p;
    }

    private static long mulDivPow10(long a, long b, int p, RoundingMode mode) {
        if (p < 0 || p > 9) {
            throw new ArithmeticException("unsupported Decimal multiply scale");
        }
        long d = POW10[p];
        long hi = Math.multiplyHigh(a, b);
        long lo = a * b;
        if (hi >= d) {
            throw new ArithmeticException("Decimal multiply overflow");
        }
        long r = 0L;
        long q = 0L;
        long limb;
        long cur;

        limb = hi >>> 32;
        cur = (r << 32) | limb;
        q = (q << 32) | (cur / d);
        r = cur % d;

        limb = hi & MASK32;
        cur = (r << 32) | limb;
        q = (q << 32) | (cur / d);
        r = cur % d;

        limb = lo >>> 32;
        cur = (r << 32) | limb;
        q = (q << 32) | (cur / d);
        r = cur % d;

        limb = lo & MASK32;
        cur = (r << 32) | limb;
        q = (q << 32) | (cur / d);
        r = cur % d;

        if (mode == RoundingMode.HALF_UP && r * 2L >= d) {
            q++;
        }
        return q;
    }

    private static long roundDiv(long dividend, long divisor, RoundingMode mode) {
        long q = dividend / divisor;
        long r = dividend % divisor;
        if (mode == RoundingMode.HALF_UP) {
            long twice = (r < 0 ? -r : r) * 2L;
            if (twice >= (divisor < 0 ? -divisor : divisor)) {
                q += (dividend < 0) == (divisor < 0) ? 1L : -1L;
            }
        }
        return q;
    }

    private static long abs(long v) {
        return v < 0L ? -v : v;
    }
}
