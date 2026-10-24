package com.ocx.aeron;

import com.ocx.book.MatchFill;
import com.ocx.codec.IoCodec;
import com.ocx.metrics.JvmMetrics;
import com.ocx.metrics.MetricsSnapshot;
import com.ocx.outbound.OutboundDepth;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.charset.StandardCharsets;

/**
 * 跨进程行格式编解码（机械同理心：定长偏移直读，零对象解析）。
 *
 * <p>入站行 = 8 字节入站时刻 + 原始 SBE OrderIn 帧（透传，撮合线程按偏移直读；
 * 2026-09-01 变长行：发布端按实际帧长 offer（帧 ≤256B，去掉定长零填充），
 * 读取端一律用消息实际长度；{@link #INBOUND_ROW_SIZE} 退化为分配上限）。
 * 出站行 = 类型字节 + 定长/变长负载
 * （成交/深度/事件信号/指标快照）。symbol 固定 16 字节 ASCII。</p>
 */
public final class AeronWire {

    /** 入站行头部：ingress 收单时刻（发布线程打点，撮合线程直读，替代原始帧缺失的时间戳）。 */
    public static final int INBOUND_TS_LENGTH = Long.BYTES;

    /** 入站行帧区：原始 SBE OrderIn 帧（含 OCX2 魔数），不足部分定长零填充。 */
    public static final int INBOUND_FRAME_LENGTH = IoCodec.MAX_ORDER_IN_FRAME_LENGTH;

    /** 入站 IPC 行上限：8B 时刻 + 256B 原始 SBE 帧（缓冲分配上限；实际行长为 8 + 实际帧长）。 */
    public static final int INBOUND_ROW_SIZE = INBOUND_TS_LENGTH + INBOUND_FRAME_LENGTH;

    public static final int MAX_OUTBOUND_ROW_SIZE = 4096;

    public static final byte TYPE_FILL = 1;
    public static final byte TYPE_DEPTH = 6;
    public static final byte TYPE_EVENT = 7;
    public static final byte TYPE_METRICS = 8;

    /** egress → metrics 流：出站发布侧统计（发布字节/背压），2026-08-30 替代 Chronicle 队列记录。 */
    public static final byte TYPE_OUTBOUND_STATS = 9;

    public static final byte TYPE_FILL_BATCH = 9;
    public static final byte TYPE_INGRESS_METRICS = 10;

    private static final int SYMBOL_LENGTH = 16;
    private static final int EVENT_NAME_LENGTH = 64;
    private static final int EVENT_DETAIL_LENGTH = 160;

    /** METRICS 行定长（发布器/订阅器按此尺寸申请缓冲）。 */
    public static final int METRICS_SIZE = 296;

    private static final int DEPTH_SIZE = 3248;

    /**
     * 单条出站消息最多打包的成交数（4096 缓冲 - 批头 5B）/ 96B 每条。
     * 注意：FILL_BATCH 4037B 超 Aeron IPC MTU（1408B）会分片（appendFragmentedMessage +
     * egress FragmentAssembler 重组）——实测分片采样仅 0.06%，而限长到 1400B 会让 offer
     * 次数 ×3（42→14 条/批），700K 边缘状态下出站路径变慢、lag 恶化 3.4 倍（7261→24736），
     * 故保持 42 条/批 + 分片（egress 已包 FragmentAssembler 兜底）。
     */
    public static final int MAX_FILLS_PER_BATCH = (MAX_OUTBOUND_ROW_SIZE - 5) / 96;

    /** ingress→core 指标行定长（consumerLag + currentTps + 拒绝增量）。 */
    public static final int INGRESS_METRICS_SIZE = 32;

    /** 出站统计行：type(1) + symbol16(16) + publishedBytes(8) + backpressure(8) + 对齐填充。 */
    public static final int OUTBOUND_STATS_SIZE = 40;

    /** 预分配清零块：字符串槽位整块清零，零分配。 */
    private static final byte[] ZEROS = new byte[512];

    private AeronWire() {}

    // ── 入站行（ingress → core）：8B 收单时刻 + 原始 SBE OrderIn 帧（透传） ──

    /** 透传编码：入站行 = [0..8) 时刻 + [8..8+length) 原始 SBE 帧（发布端按实际长度 offer）。 */
    public static void encodeInboundFrame(
            UnsafeBuffer buf, DirectBuffer frame, int offset, int length, long timestamp) {
        buf.putLong(0, timestamp);
        buf.putBytes(INBOUND_TS_LENGTH, frame, offset, length);
    }

    /** 读入站行头部时刻（core 撮合线程直读，随后按 {@link #INBOUND_TS_LENGTH} 偏移直读 SBE 帧）。 */
    public static long readInboundTimestamp(DirectBuffer buf, int offset) {
        return buf.getLong(offset);
    }

    // ── 出站行：成交 ──

    public static int encodeFill(UnsafeBuffer buf, MatchFill f, byte[] symbol16) {
        return encodeFill(buf, 0, f, symbol16);
    }

    /** 在指定偏移写入单条成交（96B 定长，含类型字节），返回结束偏移。 */
    public static int encodeFill(UnsafeBuffer buf, int offset, MatchFill f, byte[] symbol16) {
        buf.putByte(offset, TYPE_FILL);
        writeSymbol(buf, offset + 1, symbol16);
        buf.putInt(offset + 17, f.direction);
        buf.putLong(offset + 21, f.takerOrderId);
        buf.putLong(offset + 29, f.makerOrderId);
        buf.putLong(offset + 37, f.price);
        buf.putInt(offset + 45, f.priceExponent);
        buf.putLong(offset + 49, f.qty);
        buf.putInt(offset + 57, f.qtyExponent);
        buf.putLong(offset + 61, f.takerCumQuote);
        buf.putLong(offset + 69, f.makerCumQuote);
        buf.putInt(offset + 77, f.turnOverExponent);
        buf.putLong(offset + 81, f.time);
        return offset + 96;
    }

    /** 打包 count 条成交为一条消息：[TYPE_FILL_BATCH][count][96B×count]。 */
    public static int encodeFillBatch(
            UnsafeBuffer buf,
            java.util.List<MatchFill> fills,
            byte[] symbol16,
            int from,
            int count) {
        buf.putByte(0, TYPE_FILL_BATCH);
        buf.putInt(1, count);
        int offset = 5;
        for (int i = 0; i < count; i++) {
            offset = encodeFill(buf, offset, fills.get(from + i), symbol16);
        }
        return offset;
    }

    public static void decodeFill(DirectBuffer buf, int offset, MatchFill out) {
        out.direction = buf.getInt(offset + 17);
        out.symbol = readSymbol(buf, offset + 1);
        out.takerOrderId = buf.getLong(offset + 21);
        out.makerOrderId = buf.getLong(offset + 29);
        out.price = buf.getLong(offset + 37);
        out.priceExponent = buf.getInt(offset + 45);
        out.qty = buf.getLong(offset + 49);
        out.qtyExponent = buf.getInt(offset + 57);
        out.takerCumQuote = buf.getLong(offset + 61);
        out.makerCumQuote = buf.getLong(offset + 69);
        out.turnOverExponent = buf.getInt(offset + 77);
        out.time = buf.getLong(offset + 81);
    }

    /** 已知 symbol 变体：跳过 16 字节 ASCII 读取（egress 按流预绑定，零字符串分配）。 */
    public static void decodeFill(DirectBuffer buf, int offset, MatchFill out, String symbol) {
        out.direction = buf.getInt(offset + 17);
        out.symbol = symbol;
        out.takerOrderId = buf.getLong(offset + 21);
        out.makerOrderId = buf.getLong(offset + 29);
        out.price = buf.getLong(offset + 37);
        out.priceExponent = buf.getInt(offset + 45);
        out.qty = buf.getLong(offset + 49);
        out.qtyExponent = buf.getInt(offset + 57);
        out.takerCumQuote = buf.getLong(offset + 61);
        out.makerCumQuote = buf.getLong(offset + 69);
        out.turnOverExponent = buf.getInt(offset + 77);
        out.time = buf.getLong(offset + 81);
    }

    // ── 出站行：盘口深度（变长） ──

    public static int encodeDepth(UnsafeBuffer buf, OutboundDepth d, byte[] symbol16) {
        buf.putByte(0, TYPE_DEPTH);
        writeSymbol(buf, 1, symbol16);
        buf.putInt(17, d.bidCount);
        buf.putInt(21, d.askCount);
        buf.putLong(25, d.ts);
        buf.putInt(33, d.priceScale);
        buf.putInt(37, d.volumeScale);
        putLongs(buf, 41, d.bidPrices);
        putLongs(buf, 841, d.bidVolumes);
        putLongs(buf, 1641, d.askPrices);
        putLongs(buf, 2441, d.askVolumes);
        return DEPTH_SIZE;
    }

    public static void decodeDepth(DirectBuffer buf, int offset, OutboundDepth out) {
        out.symbol = readSymbol(buf, offset + 1);
        out.bidCount = buf.getInt(offset + 17);
        out.askCount = buf.getInt(offset + 21);
        out.ts = buf.getLong(offset + 25);
        out.priceScale = buf.getInt(offset + 33);
        out.volumeScale = buf.getInt(offset + 37);
        getLongs(buf, offset + 41, out.bidPrices);
        getLongs(buf, offset + 841, out.bidVolumes);
        getLongs(buf, offset + 1641, out.askPrices);
        getLongs(buf, offset + 2441, out.askVolumes);
    }

    public static void decodeDepth(DirectBuffer buf, int offset, OutboundDepth out, String symbol) {
        out.symbol = symbol;
        out.bidCount = buf.getInt(offset + 17);
        out.askCount = buf.getInt(offset + 21);
        out.ts = buf.getLong(offset + 25);
        out.priceScale = buf.getInt(offset + 33);
        out.volumeScale = buf.getInt(offset + 37);
        getLongs(buf, offset + 41, out.bidPrices);
        getLongs(buf, offset + 841, out.bidVolumes);
        getLongs(buf, offset + 1641, out.askPrices);
        getLongs(buf, offset + 2441, out.askVolumes);
    }

    // ── 出站行：关键事件信号 ──

    public static final class EventFields {
        public String symbol;
        public String event;
        public String detail;
    }

    public static int encodeEvent(UnsafeBuffer buf, byte[] symbol16, String event, String detail) {
        buf.putByte(0, TYPE_EVENT);
        writeSymbol(buf, 1, symbol16);
        writeAscii(buf, 17, EVENT_NAME_LENGTH, event);
        writeAscii(buf, 81, EVENT_DETAIL_LENGTH, detail);
        return 256;
    }

    public static void decodeEvent(DirectBuffer buf, int offset, EventFields out) {
        out.symbol = readSymbol(buf, offset + 1);
        out.event = readAscii(buf, offset + 17, EVENT_NAME_LENGTH);
        out.detail = readAscii(buf, offset + 81, EVENT_DETAIL_LENGTH);
    }

    public static void decodeEvent(DirectBuffer buf, int offset, EventFields out, String symbol) {
        out.symbol = symbol;
        out.event = readAscii(buf, offset + 17, EVENT_NAME_LENGTH);
        out.detail = readAscii(buf, offset + 81, EVENT_DETAIL_LENGTH);
    }

    // ── 出站行：指标快照 ──

    public static int encodeMetrics(UnsafeBuffer buf, MetricsSnapshot s, byte[] symbol16) {
        buf.putByte(0, TYPE_METRICS);
        writeSymbol(buf, 1, symbol16);
        buf.putLong(17, s.exportTs());
        buf.putLong(25, s.lastOrderTs());
        buf.putByte(33, (byte) (s.overloaded() ? 1 : 0));
        buf.putLong(34, s.consumerLag());
        buf.putLong(42, s.inboundBacklog());
        buf.putDouble(50, s.throughput());
        buf.putLong(58, s.ordersLimit());
        buf.putLong(66, s.ordersMarket());
        buf.putLong(74, s.tradesTotal());
        buf.putLong(82, s.rejectedTotal());
        buf.putLong(90, s.orderBookCapRejected());
        buf.putLong(98, s.openOrderCount());
        buf.putLong(106, s.orderBookCap());
        buf.putInt(114, s.depthBid());
        buf.putInt(118, s.depthAsk());
        buf.putDouble(122, s.durationP50());
        buf.putDouble(130, s.durationP95());
        buf.putDouble(138, s.durationP99());
        buf.putDouble(146, s.durationP999());
        buf.putDouble(154, s.durationMax());
        buf.putLong(162, s.batchSizeMax());
        putJvm(buf, 170, s.jvm());
        buf.putLong(258, s.slotsUsed());
        buf.putLong(266, s.outboundBackpressure());
        buf.putLong(274, s.buyOrders());
        buf.putLong(282, s.sellOrders());
        return METRICS_SIZE;
    }

    public static MetricsSnapshot decodeMetrics(DirectBuffer buf, int offset) {
        String symbol = readSymbol(buf, offset + 1);
        return new MetricsSnapshot(
                symbol,
                buf.getLong(offset + 17),
                buf.getLong(offset + 25),
                buf.getByte(offset + 33) != 0,
                buf.getLong(offset + 34),
                buf.getLong(offset + 42),
                buf.getDouble(offset + 50),
                buf.getLong(offset + 58),
                buf.getLong(offset + 66),
                buf.getLong(offset + 74),
                buf.getLong(offset + 82),
                buf.getLong(offset + 90),
                buf.getLong(offset + 98),
                buf.getLong(offset + 106),
                buf.getInt(offset + 114),
                buf.getInt(offset + 118),
                buf.getDouble(offset + 122),
                buf.getDouble(offset + 130),
                buf.getDouble(offset + 138),
                buf.getDouble(offset + 146),
                buf.getDouble(offset + 154),
                buf.getLong(offset + 162),
                getJvm(buf, offset + 170),
                buf.getLong(offset + 258),
                buf.getLong(offset + 266),
                buf.getLong(offset + 274),
                buf.getLong(offset + 282));
    }

    // ── ingress→core 指标行（三进程模式：入站侧 lag/TPS/拒绝合流进核心快照）──

    public static int encodeIngressMetrics(
            UnsafeBuffer buf, long consumerLag, double currentTps, long rejectedTotal) {
        buf.putByte(0, TYPE_INGRESS_METRICS);
        buf.putLong(1, consumerLag);
        buf.putDouble(9, currentTps);
        buf.putLong(17, rejectedTotal);
        return INGRESS_METRICS_SIZE;
    }

    public record IngressMetricsRow(long consumerLag, double currentTps, long rejectedTotal) {}

    public static IngressMetricsRow decodeIngressMetrics(DirectBuffer buf, int offset) {
        return new IngressMetricsRow(
                buf.getLong(offset + 1), buf.getDouble(offset + 9), buf.getLong(offset + 17));
    }

    // ── 出站行：egress 发布侧统计（sidecar 直接消费，不再经 Chronicle 队列）──

    public record OutboundStatsRow(String symbol, long publishedBytes, long backpressure) {}

    public static int encodeOutboundStats(
            UnsafeBuffer buf, byte[] symbol16, long publishedBytes, long backpressure) {
        buf.putByte(0, TYPE_OUTBOUND_STATS);
        writeSymbol(buf, 1, symbol16);
        buf.putLong(17, publishedBytes);
        buf.putLong(25, backpressure);
        return OUTBOUND_STATS_SIZE;
    }

    public static OutboundStatsRow decodeOutboundStats(DirectBuffer buf, int offset) {
        return new OutboundStatsRow(
                readSymbol(buf, offset + 1), buf.getLong(offset + 17), buf.getLong(offset + 25));
    }

    // ── 辅助 ──

    private static void putJvm(UnsafeBuffer buf, int offset, JvmMetrics jvm) {
        buf.putLong(offset, jvm.heapUsed());
        buf.putLong(offset + 8, jvm.heapCommitted());
        buf.putLong(offset + 16, jvm.heapMax());
        buf.putLong(offset + 24, jvm.nonHeapUsed());
        buf.putLong(offset + 32, jvm.threadsLive());
        buf.putLong(offset + 40, jvm.threadsDaemon());
        buf.putLong(offset + 48, jvm.threadsPeak());
        buf.putLong(offset + 56, jvm.gcCollections());
        buf.putLong(offset + 64, jvm.gcCollectionTimeMillis());
        buf.putLong(offset + 72, jvm.gcLastPauseMillis());
        buf.putLong(offset + 80, jvm.uptimeMillis());
    }

    private static JvmMetrics getJvm(DirectBuffer buf, int offset) {
        return new JvmMetrics(
                buf.getLong(offset),
                buf.getLong(offset + 8),
                buf.getLong(offset + 16),
                buf.getLong(offset + 24),
                buf.getLong(offset + 32),
                buf.getLong(offset + 40),
                buf.getLong(offset + 48),
                buf.getLong(offset + 56),
                buf.getLong(offset + 64),
                buf.getLong(offset + 72),
                buf.getLong(offset + 80));
    }

    private static void putLongs(UnsafeBuffer buf, int offset, long[] values) {
        for (int i = 0; i < values.length; i++) {
            buf.putLong(offset + i * 8, values[i]);
        }
    }

    private static void getLongs(DirectBuffer buf, int offset, long[] out) {
        for (int i = 0; i < out.length; i++) {
            out[i] = buf.getLong(offset + i * 8);
        }
    }

    /** 预编码 symbol 为 16 字节 ASCII（sink 构造期缓存，热路径零 getBytes/零分配）。 */
    public static byte[] symbolBytes(String symbol) {
        byte[] out = new byte[SYMBOL_LENGTH];
        if (symbol != null) {
            byte[] src = symbol.getBytes(StandardCharsets.US_ASCII);
            System.arraycopy(src, 0, out, 0, Math.min(src.length, SYMBOL_LENGTH));
        }
        return out;
    }

    /** 整块写入预编码 symbol（定长 16 字节，无需清零）。 */
    private static void writeSymbol(UnsafeBuffer buf, int offset, byte[] symbol16) {
        buf.putBytes(offset, symbol16, 0, SYMBOL_LENGTH);
    }

    private static String readSymbol(DirectBuffer buf, int offset) {
        return readAscii(buf, offset, SYMBOL_LENGTH);
    }

    private static void writeAscii(UnsafeBuffer buf, int offset, int length, String value) {
        buf.putBytes(offset, ZEROS, 0, length); // 整块清零（预分配块，零分配）
        if (value != null) {
            byte[] bytes = value.getBytes(StandardCharsets.US_ASCII);
            buf.putBytes(offset, bytes, 0, Math.min(bytes.length, length));
        }
    }

    private static String readAscii(DirectBuffer buf, int offset, int length) {
        int end = offset;
        int limit = offset + length;
        while (end < limit && buf.getByte(end) != 0) {
            end++;
        }
        byte[] bytes = new byte[end - offset];
        buf.getBytes(offset, bytes);
        return new String(bytes, StandardCharsets.US_ASCII);
    }
}
