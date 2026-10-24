package com.ocx.snapshot;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * 快照 recording 编解码（2026-08-30，快照迁往 Aeron Archive recording；Chronicle 版已删）。
 *
 * <p>消息格式（每条 Aeron 消息）：{@code [4B 魔数 SNP1][4B type][payload]}：</p>
 * <ul>
 *   <li>{@link #TYPE_META}=0：payload 固定 40B（ts / coinScale / baseCoinScale /
 *       inboundRecordingId / inboundPosition / orderCount / triggerCount），首条消息；</li>
 *   <li>{@link #TYPE_ORDER_BATCH}=1 / {@link #TYPE_STOP_BATCH}=2：payload =
 *       {@code [4B count][record...]}，每条 record = {@code [4B len][OCX1 SBE 负载]}
 *       （复用 {@link OrderRecordCodec}，与旧 Chronicle 快照/WAL 同一订单编码）；</li>
 *   <li>{@link #TYPE_END}=3：payload = {@code [4B orderCount][4B triggerCount]}，尾条消息。</li>
 * </ul>
 *
 * <p>完整性语义（对齐旧 END marker）：恢复端首条必须 META、尾条必须 END 且计数一致；
 * END 缺失（录制中途崩溃/截断）或计数不符即弃用该 snapshot recording，回退上一份
 * 或全量回放入站。</p>
 */
public final class SnapshotCodec {

    public static final byte[] MAGIC = {'S', 'N', 'P', '1'};
    public static final int MAGIC_LEN = 4;
    public static final int TYPE_LEN = Integer.BYTES;
    public static final int HEADER_LEN = MAGIC_LEN + TYPE_LEN;

    public static final int TYPE_META = 0;
    public static final int TYPE_ORDER_BATCH = 1;
    public static final int TYPE_STOP_BATCH = 2;
    public static final int TYPE_END = 3;

    /** 快照段起点 marker（2026-08-31 常驻 recording 分段）：每张快照 =
     * BEGIN(seq) + META + ORDER + STOP + END；恢复/对账端按 seq 定位最新有效段。 */
    public static final int TYPE_SNAPSHOT_BEGIN = 4;

    /** META payload：ts(8) + coinScale(4) + baseCoinScale(4) + inboundRecordingId(8) + inboundPosition(8) + orderCount(4) + triggerCount(4)。 */
    public static final int META_BODY_LEN = 8 + 4 + 4 + 8 + 8 + 4 + 4;

    public static final int META_LEN = HEADER_LEN + META_BODY_LEN;

    /** END payload：orderCount(4) + triggerCount(4)。 */
    public static final int END_BODY_LEN = 8;

    public static final int END_LEN = HEADER_LEN + END_BODY_LEN;

    /** BEGIN payload：快照 seq(8)。 */
    public static final int BEGIN_BODY_LEN = Long.BYTES;

    public static final int BEGIN_LEN = HEADER_LEN + BEGIN_BODY_LEN;

    /** 批量帧最大字节（含头部），超限前 flush；单条 Aeron 消息大小可控。 */
    public static final int MAX_BATCH_BYTES = 16 * 1024;

    /** 批量帧头部：header + count(4)。 */
    public static final int BATCH_HEADER_LEN = HEADER_LEN + Integer.BYTES;

    private SnapshotCodec() {}

    /**
     * 快照元数据（snapshot recording 首条消息）。
     *
     * @param count 在簿订单数
     * @param triggerCount 在挂条件单数
     * @param inboundRecordingId 入站 recording id（锚点；-1 = 无锚点，全量回放入站）
     * @param inboundPosition 入站 recording 内字节位置（min(撮合消费, recorder 已录)）
     */
    public record SnapshotMeta(
            String symbol,
            long timestamp,
            int coinScale,
            int baseCoinScale,
            int count,
            int triggerCount,
            long inboundRecordingId,
            long inboundPosition) {}

    public static int encodeMeta(MutableDirectBuffer b, int off, SnapshotMeta m) {
        b.putBytes(off, MAGIC);
        b.putInt(off + MAGIC_LEN, TYPE_META);
        int p = off + HEADER_LEN;
        b.putLong(p, m.timestamp());
        b.putInt(p + 8, m.coinScale());
        b.putInt(p + 12, m.baseCoinScale());
        b.putLong(p + 16, m.inboundRecordingId());
        b.putLong(p + 24, m.inboundPosition());
        b.putInt(p + 32, m.count());
        b.putInt(p + 36, m.triggerCount());
        return META_LEN;
    }

    public static int encodeEnd(MutableDirectBuffer b, int off, int orderCount, int triggerCount) {
        b.putBytes(off, MAGIC);
        b.putInt(off + MAGIC_LEN, TYPE_END);
        int p = off + HEADER_LEN;
        b.putInt(p, orderCount);
        b.putInt(p + 4, triggerCount);
        return END_LEN;
    }

    /** 快照段起点：MAGIC + type + seq(8)。 */
    public static int encodeBegin(MutableDirectBuffer b, int off, long seq) {
        b.putBytes(off, MAGIC);
        b.putInt(off + MAGIC_LEN, TYPE_SNAPSHOT_BEGIN);
        b.putLong(off + HEADER_LEN, seq);
        return BEGIN_LEN;
    }

    public static long decodeBeginSeq(DirectBuffer b, int off) {
        return b.getLong(off + HEADER_LEN);
    }

    /** 批量帧头部（魔数 + type + 记录数）；随后逐条 {@code [4B len][OCX1 负载]} 追加。 */
    public static int encodeBatchHeader(MutableDirectBuffer b, int off, int type, int count) {
        b.putBytes(off, MAGIC);
        b.putInt(off + MAGIC_LEN, type);
        b.putInt(off + HEADER_LEN, count);
        return BATCH_HEADER_LEN;
    }

    /** 追加一条 OCX1 负载到批量帧，返回追加后总长（超出 MAX_BATCH_BYTES 返回 -1，调用方先 flush）。 */
    public static int appendRecord(
            MutableDirectBuffer batch,
            int batchOffset,
            int currentLen,
            DirectBuffer payload,
            int payloadOffset,
            int payloadLen) {
        int need = Integer.BYTES + payloadLen;
        if (batchOffset + currentLen + need > MAX_BATCH_BYTES) {
            return -1;
        }
        int p = batchOffset + currentLen;
        batch.putInt(p, payloadLen);
        batch.putBytes(p + Integer.BYTES, payload, payloadOffset, payloadLen);
        return currentLen + need;
    }

    public static int type(DirectBuffer b, int off) {
        return b.getInt(off + MAGIC_LEN);
    }

    public static boolean hasMagic(DirectBuffer b, int off) {
        for (int i = 0; i < MAGIC_LEN; i++) {
            if (b.getByte(off + i) != MAGIC[i]) {
                return false;
            }
        }
        return true;
    }

    public static SnapshotMeta decodeMeta(String symbol, DirectBuffer b, int off) {
        int p = off + HEADER_LEN;
        return new SnapshotMeta(
                symbol,
                b.getLong(p),
                b.getInt(p + 8),
                b.getInt(p + 12),
                b.getInt(p + 32),
                b.getInt(p + 36),
                b.getLong(p + 16),
                b.getLong(p + 24));
    }

    /** END 计数：返回 {@code int[2] = {orderCount, triggerCount}}。 */
    public static int[] decodeEndCounts(DirectBuffer b, int off) {
        int p = off + HEADER_LEN;
        return new int[] {b.getInt(p), b.getInt(p + 4)};
    }

    public static int batchCount(DirectBuffer b, int off) {
        return b.getInt(off + HEADER_LEN);
    }

    /** 遍历批量帧内全部 record（payload 偏移/长度；offset 为消息起始）。 */
    public static void forEachRecord(DirectBuffer b, int off, int length, RecordConsumer consumer) {
        int p = off + BATCH_HEADER_LEN;
        int end = off + length;
        int count = batchCount(b, off);
        for (int i = 0; i < count && p < end; i++) {
            if (end - p < Integer.BYTES) {
                break;
            }
            int len = b.getInt(p);
            p += Integer.BYTES;
            if (len <= 0 || end - p < len) {
                break;
            }
            consumer.accept(p, len);
            p += len;
        }
    }

    @FunctionalInterface
    public interface RecordConsumer {
        void accept(int payloadOffset, int payloadLength);
    }
}
