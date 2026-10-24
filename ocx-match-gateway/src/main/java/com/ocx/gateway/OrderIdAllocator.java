package com.ocx.gateway;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.function.LongSupplier;

/**
 * Snowflake 化 orderId 发号器（2026-09-01，规则见 docs/02-data-structure.md §1.3（发号规则；完整设计见 §1.9））：
 * {@code orderId = (seg << 42) | (issuerId << 34) | counter}，
 * orderId 为**纯业务号**（2026-09-01 起不再内嵌槽位）；引擎内部经
 * orderId→slot 映射（Long2IntMap）+ 槽位 freelist 落位，见 02-data-structure.md §1.3。
 *
 * <p>位分配（63 位，不越 i64 正数域）：</p>
 * <pre>
 * 62                   42 41      34 33     23 22     0
 * +---------------------+----------+---------+---------+
 * | 时间段 seg (21 位)   | issuer(8)| gen(11) | slot(23)|
 * +---------------------+----------+---------+---------+
 * </pre>
 *
 * <p>{@code seg} = 自 1970-01-01 起的小时数（{@link System#currentTimeMillis()} 推导），
 * 单调不减：时钟回拨时保持已持久化段、同段内 counter 继续递增（不撞历史）；
 * 段内 counter 耗尽（2^34-1）时提前推进一段并从 1 重发（高位随时间/耗量单调涨，
 * 永不撞历史段）。低 34 位 counter 只是段内序号，与引擎槽位完全解耦。</p>
 *
 * <p>状态持久化（stateFile）：{@code seg counter} 两行，重启同段从水位续发、
 * 跨段重发；水位与段均单调，杜绝重启撞号。</p>
 */
public final class OrderIdAllocator {

    /** 低位计数区位宽；单段单发号方容量 2^34（与引擎槽位无关，纯发号序号）。 */
    public static final int COUNTER_BITS = 34;

    /** 发号方 ID 位宽（1..255，部署内唯一）。 */
    public static final int ISSUER_BITS = 8;

    /** 时间段位宽（小时粒度，2^21 ≈ 239 年）。 */
    public static final int SEG_BITS = 21;

    public static final long ISSUER_MAX = (1L << ISSUER_BITS) - 1;
    public static final long COUNTER_MAX = (1L << COUNTER_BITS) - 1;
    private static final long SEG_MASK = (1L << SEG_BITS) - 1;

    /** 段内毫秒数（1 小时一段）。 */
    public static final long SEG_MS = 3_600_000L;

    /** seg 左移位数 = ISSUER_BITS + COUNTER_BITS。 */
    private static final int SEG_SHIFT = ISSUER_BITS + COUNTER_BITS;

    private final long issuerId;
    private final Path stateFile;
    private final LongSupplier clock;

    /** 当前时间段（小时索引，单调不减；持久化后重启续用）。 */
    private long seg;

    /** 本段内计数（1 起；段切换/提前推进时归零）。 */
    private long counter;

    public OrderIdAllocator(long issuerId, Path stateFile) {
        this(issuerId, stateFile, System::currentTimeMillis);
    }

    /** 包内可见：注入时钟（测试用），语义与默认构造一致。 */
    OrderIdAllocator(long issuerId, Path stateFile, LongSupplier clock) {
        if (issuerId < 1 || issuerId > ISSUER_MAX) {
            throw new IllegalArgumentException("issuerId 必须在 1.." + ISSUER_MAX);
        }
        this.issuerId = issuerId;
        this.stateFile = stateFile;
        this.clock = clock;
        State s = loadState(stateFile);
        // 时钟回拨保护：seg 只取 max；同段续发 counter，新段从 0 重发
        this.seg = Math.max(clockHour(clock.getAsLong()), s.seg);
        this.counter = (this.seg == s.seg) ? s.counter : 0L;
    }

    /** 分配下一个 orderId（幂等/并发安全），并持久化 (seg, counter) 水位。 */
    public synchronized long allocate() {
        long nowHour = clockHour(clock.getAsLong());
        if (nowHour > seg) {
            // 自然段切换：新段 counter 从 0 重发（高位段不同，不撞历史）
            seg = nowHour;
            counter = 0L;
        }
        if (counter >= COUNTER_MAX) {
            // 段内耗尽：提前推进一段并重发（高位单调涨，永不撞历史段）
            seg++;
            counter = 0L;
        }
        if (seg > SEG_MASK) {
            // 理论边界（239 年自然时间 / 极端 TPS 数百年后）：拒绝发号而非产出负数 id
            throw new IllegalStateException("时间段 seg 耗尽: " + seg);
        }
        counter++;
        persistWatermark();
        return (seg << SEG_SHIFT) | (issuerId << COUNTER_BITS) | counter;
    }

    public synchronized long currentCounter() {
        return counter;
    }

    /** 当前时间段（小时索引；用于展示/测试，非热路径）。 */
    public synchronized long segment() {
        return seg;
    }

    public long issuerId() {
        return issuerId;
    }

    private static long clockHour(long nowMillis) {
        return (nowMillis / SEG_MS) & SEG_MASK;
    }

    private static State loadState(Path stateFile) {
        try {
            if (stateFile != null && Files.exists(stateFile)) {
                List<String> lines =
                        Files.readAllLines(stateFile, StandardCharsets.UTF_8).stream()
                                .filter(s -> !s.isBlank())
                                .toList();
                if (!lines.isEmpty()) {
                    String last = lines.get(lines.size() - 1).trim();
                    String[] parts = last.split("\\s+");
                    long seg = parts.length >= 2 ? Long.parseLong(parts[0]) : 0L;
                    long counter = Long.parseLong(parts[parts.length - 1]);
                    return new State(seg, counter);
                }
            }
        } catch (IOException | NumberFormatException ignored) {
            // 水位文件损坏/缺失：从当前时间段 0 计数开始（生产应告警，此处尽力恢复）
        }
        return new State(0L, 0L);
    }

    private void persistWatermark() {
        if (stateFile == null) {
            return;
        }
        try {
            Files.createDirectories(stateFile.getParent());
            Files.writeString(
                    stateFile,
                    seg + " " + counter + "\n",
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE);
        } catch (IOException ignored) {
            // 发号不因持久化失败中断（生产应告警）；重启水位回退有撞号风险
        }
    }

    private record State(long seg, long counter) {}
}
