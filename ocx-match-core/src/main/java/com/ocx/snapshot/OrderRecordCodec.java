package com.ocx.snapshot;

import com.ocx.codec.sbe.wal.DecimalValueEncoder;
import com.ocx.codec.sbe.wal.MessageHeaderDecoder;
import com.ocx.codec.sbe.wal.MessageHeaderEncoder;
import com.ocx.codec.sbe.wal.OrderCreateDecoder;
import com.ocx.codec.sbe.wal.OrderCreateEncoder;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * WAL ORDER_CREATE 订单负载编解码（SBE 二进制格式）。
 *
 * <p>线上负载布局：4 字节魔数 {@code OCX1} + SBE OrderCreate 消息。
 * 负载始终为 SBE 二进制（2026-08-21 起，旧 JSON 文本格式已彻底移除）。</p>
 *
 * <p>编码目标：热路径零 JSON 分配。调用方（SnapshotCodec）持有可复用
 * {@link MutableDirectBuffer} 与字符串 scratch，本类只往 buffer 里写，
 * 不产生中间 String/byte[]。若字段无法装入定长布局（如 Decimal exponent 溢出 int8
 * mantissa），返回 -1，由调用方回退旧 JSON 文本格式，保证不丢数据。</p>
 */
public final class OrderRecordCodec {

    /** 二进制格式魔数：OCX + 版本 1。格式演进时递增末位。 */
    public static final byte[] MAGIC = {'O', 'C', 'X', '1'};

    public static final int MAGIC_LEN = MAGIC.length;

    /** 单条消息最大预期长度（固定块 164 + 全部 var 数据）。超限视为编码失败回退 JSON。 */
    public static final int MAX_ENCODED_LENGTH = 1024;

    private OrderRecordCodec() {}

    private static final String[] TIF_STRINGS = {"GTC", "IOC", "FOK", "POST_ONLY"};
    private static final String[] STOP_TYPE_STRINGS = {
        "STOP_LIMIT", "STOP_MARKET", "TAKE_PROFIT_LIMIT", "TAKE_PROFIT_MARKET", "TRAILING_STOP"
    };

    public static boolean isSbe(byte[] payload) {
        if (payload == null || payload.length < MAGIC_LEN) return false;
        for (int i = 0; i < MAGIC_LEN; i++) {
            if (payload[i] != MAGIC[i]) return false;
        }
        return true;
    }

    /**
     * 纵表链路字段版编码（ORDER_CREATE）：入簿订单的定点字段直接写现有 SBE schema，
     * price/origQty 的 exponent 隐含 0（恢复按 symbol scale 归一化，幂等）。
     * 热路径复用 buffer/strScratch/header/enc，零分配；失败返回 -1。
     */
    public static int encodeFields(
            String symbol,
            long orderId,
            long userId,
            long price,
            long origQty,
            int direction,
            int tif,
            long time,
            long execQty,
            long cumQuote,
            MutableDirectBuffer buffer,
            int offset,
            byte[] strScratch,
            MessageHeaderEncoder header,
            OrderCreateEncoder enc) {
        if (buffer == null || symbol == null) return -1;
        if (offset + MAGIC_LEN + MAX_ENCODED_LENGTH > buffer.capacity()) return -1;

        buffer.putBytes(offset, MAGIC);
        enc.wrapAndApplyHeader(buffer, offset + MAGIC_LEN, header);

        // —— 按 schema 声明顺序写入 ——
        enc.orderId(orderId);
        enc.userId(userId == Long.MIN_VALUE ? 0L : userId);
        if (!putDecimal(enc.origQty(), origQty, 0)) return -1;
        if (!putDecimal(enc.executedQty(), execQty, 0)) return -1;
        if (!putDecimal(enc.cumQuoteQty(), cumQuote, 0)) return -1;

        enc.coinSymbolPresent((short) 0);
        enc.baseSymbolPresent((short) 0);
        enc.orderStatusPresent((short) 0);
        enc.orderStatus((short) 0);

        enc.orderDirection(
                direction == 0
                        ? com.ocx.codec.sbe.wal.OrderDirection.BUY
                        : com.ocx.codec.sbe.wal.OrderDirection.SELL);

        enc.reduceOnly((short) 0);
        if (!putDecimal(enc.price(), price, 0)) return -1;

        enc.time(time);
        enc.completedTime(0L);
        enc.cancelTime(0L);
        enc.cancelOrder((short) 0);

        enc.traceIdPresent((short) 0);
        enc.visibleQtyPresent((short) 0);
        enc.displayedQtyPresent((short) 0);
        enc.triggerPricePresent((short) 0);
        enc.triggerOrderTypePresent((short) 0);
        enc.trailDeltaPresent((short) 0);
        enc.batchCancelUserId(0L);

        // ── var 数据段（schema 声明顺序，2026-09-01 修复：生成 accessor 把长度前缀
        //    写在固定块内、缺失槽位被跳过，导致快照记录无法解码——改为手写自洽段）──
        if (!writeVar(enc, buffer, symbol, strScratch)) return -1;
        if (!writeAbsentVar(enc, buffer)) return -1;
        if (!writeAbsentVar(enc, buffer)) return -1;
        if (!writeVar(enc, buffer, "LIMIT_PRICE", strScratch)) return -1;
        if (!writeVar(
                enc,
                buffer,
                tif >= 0 && tif < TIF_STRINGS.length ? TIF_STRINGS[tif] : TIF_STRINGS[0],
                strScratch)) return -1;
        if (!writeAbsentVar(enc, buffer)) return -1;
        if (!writeAbsentVar(enc, buffer)) return -1;

        return MAGIC_LEN + MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
    }

    /**
     * 快照条件单字段版编码（ORDER_CREATE schema 承载 STOP 语义）：
     * orderType="STOP" 标记，triggerOrderType/triggerPrice/trailDelta 用 schema 现成字段；
     * 定点 exponent 隐含 0（恢复直接使用）。热路径复用 buffer/strScratch/header/enc。
     */
    public static int encodeStopFields(
            String symbol,
            long orderId,
            long userId,
            long triggerPrice,
            long price,
            long origQty,
            int direction,
            int triggerType,
            long trailDelta,
            long time,
            MutableDirectBuffer buffer,
            int offset,
            byte[] strScratch,
            MessageHeaderEncoder header,
            OrderCreateEncoder enc) {
        if (buffer == null
                || symbol == null
                || triggerType < 0
                || triggerType >= STOP_TYPE_STRINGS.length) {
            return -1;
        }
        if (offset + MAGIC_LEN + MAX_ENCODED_LENGTH > buffer.capacity()) {
            return -1;
        }

        buffer.putBytes(offset, MAGIC);
        enc.wrapAndApplyHeader(buffer, offset + MAGIC_LEN, header);

        enc.orderId(orderId);
        enc.userId(userId == Long.MIN_VALUE ? 0L : userId);
        if (!putDecimal(enc.origQty(), origQty, 0)) return -1;
        if (!putDecimal(enc.executedQty(), 0L, 0)) return -1;
        if (!putDecimal(enc.cumQuoteQty(), 0L, 0)) return -1;

        enc.coinSymbolPresent((short) 0);
        enc.baseSymbolPresent((short) 0);
        enc.orderStatusPresent((short) 0);
        enc.orderStatus((short) 0);

        enc.orderDirection(
                direction == 0
                        ? com.ocx.codec.sbe.wal.OrderDirection.BUY
                        : com.ocx.codec.sbe.wal.OrderDirection.SELL);

        enc.reduceOnly((short) 0);
        if (!putDecimal(enc.price(), price, 0)) return -1;

        enc.time(time);
        enc.completedTime(0L);
        enc.cancelTime(0L);
        enc.cancelOrder((short) 0);

        enc.traceIdPresent((short) 0);
        enc.visibleQtyPresent((short) 0);
        enc.displayedQtyPresent((short) 0);
        enc.triggerPricePresent((short) 1);
        if (!putDecimal(enc.triggerPrice(), triggerPrice, 0)) return -1;
        enc.trailDeltaPresent((short) 1);
        if (!putDecimal(enc.trailDelta(), trailDelta, 0)) return -1;
        enc.batchCancelUserId(0L);

        // ── var 数据段（schema 声明顺序；与 decodeStopFields 严格对齐）──
        if (!writeVar(enc, buffer, symbol, strScratch)) return -1;
        if (!writeAbsentVar(enc, buffer)) return -1;
        if (!writeAbsentVar(enc, buffer)) return -1;
        if (!writeVar(enc, buffer, "STOP", strScratch)) return -1;
        if (!writeVar(enc, buffer, "GTC", strScratch)) return -1;
        if (!writeAbsentVar(enc, buffer)) return -1;
        if (!writeVar(enc, buffer, STOP_TYPE_STRINGS[triggerType], strScratch)) return -1;

        return MAGIC_LEN + MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
    }

    /** 纵表链路字段版解码（恢复路径）：返回定点字段（旧文件原始 exponent 亦返回，
     *  由恢复侧按 symbol scale 归一化）。 */
    public static SnapshotOrderFields decodeFields(byte[] payload) {
        if (!isSbe(payload)) {
            throw new IllegalArgumentException("payload 不是 SBE 格式（缺少魔数 OCX1）");
        }
        UnsafeBuffer buffer = new UnsafeBuffer(payload);
        MessageHeaderDecoder header = new MessageHeaderDecoder();
        header.wrap(buffer, MAGIC_LEN);
        if (header.templateId() != OrderCreateDecoder.TEMPLATE_ID) {
            throw new IllegalArgumentException("未知 SBE templateId: " + header.templateId());
        }
        OrderCreateDecoder dec = new OrderCreateDecoder();
        dec.wrap(
                buffer,
                MAGIC_LEN + MessageHeaderDecoder.ENCODED_LENGTH,
                header.blockLength(),
                header.version());

        SnapshotOrderFields f = new SnapshotOrderFields();
        f.orderId = dec.orderId();
        long userId = dec.userId();
        f.userId = userId == 0L ? Long.MIN_VALUE : userId;
        // var 数据必须按 schema 声明顺序读取（含跳过），否则 dec.limit() 错位
        f.symbol = readVar(dec, buffer);
        readVar(dec, buffer);
        readVar(dec, buffer);
        readVar(dec, buffer);
        String tif = readVar(dec, buffer);
        readVar(dec, buffer);
        readVar(dec, buffer);
        f.origQty = dec.origQty().mantissa();
        f.origQtyExponent = dec.origQty().exponent();
        f.executedQty = dec.executedQty().mantissa();
        f.cumQuoteQty = dec.cumQuoteQty().mantissa();
        f.price = dec.price().mantissa();
        f.priceExponent = dec.price().exponent();
        f.direction = dec.orderDirection() == com.ocx.codec.sbe.wal.OrderDirection.BUY ? 0 : 1;
        f.time = dec.time();
        f.tif =
                switch (tif == null ? "" : tif) {
                    case "IOC" -> 1;
                    case "FOK" -> 2;
                    case "POST_ONLY" -> 3;
                    default -> 0;
                };
        return f;
    }

    /** 快照条件单字段版解码（恢复路径）：按 schema var 声明顺序推进 limit 后直读 fixed 字段。 */
    public static TriggerSnapshotFields decodeStopFields(byte[] payload) {
        if (!isSbe(payload)) {
            throw new IllegalArgumentException("payload 不是 SBE 格式（缺少魔数 OCX1）");
        }
        UnsafeBuffer buffer = new UnsafeBuffer(payload);
        MessageHeaderDecoder header = new MessageHeaderDecoder();
        header.wrap(buffer, MAGIC_LEN);
        if (header.templateId() != OrderCreateDecoder.TEMPLATE_ID) {
            throw new IllegalArgumentException("未知 SBE templateId: " + header.templateId());
        }
        OrderCreateDecoder dec = new OrderCreateDecoder();
        dec.wrap(
                buffer,
                MAGIC_LEN + MessageHeaderDecoder.ENCODED_LENGTH,
                header.blockLength(),
                header.version());

        // var 数据必须按 schema 声明顺序读取（含跳过），否则 dec.limit() 错位
        readVar(dec, buffer);
        readVar(dec, buffer);
        readVar(dec, buffer);
        readVar(dec, buffer);
        readVar(dec, buffer);
        readVar(dec, buffer);
        String triggerTypeStr = readVar(dec, buffer);

        long userId = dec.userId();
        int triggerType = -1;
        for (int i = 0; i < STOP_TYPE_STRINGS.length; i++) {
            if (STOP_TYPE_STRINGS[i].equals(triggerTypeStr)) {
                triggerType = i;
                break;
            }
        }
        if (triggerType < 0) {
            throw new IllegalArgumentException("未知 triggerOrderType: " + triggerTypeStr);
        }
        return new TriggerSnapshotFields(
                dec.orderId(),
                userId == 0L ? Long.MIN_VALUE : userId,
                dec.triggerPrice().mantissa(),
                dec.price().mantissa(),
                dec.origQty().mantissa(),
                dec.orderDirection() == com.ocx.codec.sbe.wal.OrderDirection.BUY ? 0 : 1,
                triggerType,
                dec.trailDelta().mantissa(),
                dec.time());
    }

    // ============ varString 辅助（2026-09-01 修复）============
    //
    // schema（ocx-wal-schema.xml）把 varString 声明在固定字段中间，SBE 生成的
    // accessor 把长度前缀定位到固定块内部（offset+16），旧 putString 又跳过缺失
    // 槽位——快照 ORDER/STOP 记录全部无法解码（预填静默为空，对账锚点模式暴露为
    // U5/U1 尾部失败）。修复：绕开生成 accessor，var 段按 schema 声明顺序手写
    // [4B 长度][UTF-8 数据]（缺失槽位长度=0），编解码严格对齐。

    private static final int VAR_LENGTH_BYTES = Integer.BYTES;

    /** 写入一个 var 槽位：[4B 长度][数据]，追加到 enc.limit()。返回 false 表示放不下。 */
    private static boolean writeVar(
            OrderCreateEncoder enc, MutableDirectBuffer buffer, String s, byte[] scratch) {
        int n;
        if (s == null) {
            n = 0;
        } else {
            if (s.length() > scratch.length) return false;
            n = utf8(s, scratch, 0);
            if (n < 0 || n > scratch.length) return false;
        }
        int dataPos = enc.limit();
        if (dataPos + VAR_LENGTH_BYTES + n > buffer.capacity()) return false;
        buffer.putInt(dataPos, n, java.nio.ByteOrder.LITTLE_ENDIAN);
        if (n > 0) {
            buffer.putBytes(dataPos + VAR_LENGTH_BYTES, scratch, 0, n);
        }
        enc.limit(dataPos + VAR_LENGTH_BYTES + n);
        return true;
    }

    /** 写入缺失 var 槽位（长度 0）。 */
    private static boolean writeAbsentVar(OrderCreateEncoder enc, MutableDirectBuffer buffer) {
        int dataPos = enc.limit();
        if (dataPos + VAR_LENGTH_BYTES > buffer.capacity()) return false;
        buffer.putInt(dataPos, 0, java.nio.ByteOrder.LITTLE_ENDIAN);
        enc.limit(dataPos + VAR_LENGTH_BYTES);
        return true;
    }

    /** 读取一个 var 槽位：[4B 长度][数据]，从 dec.limit() 推进。 */
    private static String readVar(OrderCreateDecoder dec, DirectBuffer buffer) {
        int dataPos = dec.limit();
        int n = buffer.getInt(dataPos, java.nio.ByteOrder.LITTLE_ENDIAN);
        dec.limit(dataPos + VAR_LENGTH_BYTES + n);
        if (n <= 0) {
            return "";
        }
        return buffer.getStringWithoutLengthUtf8(dataPos + VAR_LENGTH_BYTES, n);
    }

    /** 手动 UTF-8 编码（避免 String.getBytes 分配）。scratch 不足时返回 -1。 */
    private static int utf8(CharSequence s, byte[] out, int start) {
        int p = start;
        int len = s.length();
        for (int i = 0; i < len; i++) {
            char c = s.charAt(i);
            if (c < 0x80) {
                if (p + 1 > out.length) return -1;
                out[p++] = (byte) c;
            } else if (c < 0x800) {
                if (p + 2 > out.length) return -1;
                out[p++] = (byte) (0xC0 | (c >> 6));
                out[p++] = (byte) (0x80 | (c & 0x3F));
            } else if (Character.isHighSurrogate(c)
                    && i + 1 < len
                    && Character.isLowSurrogate(s.charAt(i + 1))) {
                if (p + 4 > out.length) return -1;
                int cp = Character.toCodePoint(c, s.charAt(i + 1));
                out[p++] = (byte) (0xF0 | (cp >> 18));
                out[p++] = (byte) (0x80 | ((cp >> 12) & 0x3F));
                out[p++] = (byte) (0x80 | ((cp >> 6) & 0x3F));
                out[p++] = (byte) (0x80 | (cp & 0x3F));
                i++;
            } else {
                if (p + 3 > out.length) return -1;
                out[p++] = (byte) (0xE0 | (c >> 12));
                out[p++] = (byte) (0x80 | ((c >> 6) & 0x3F));
                out[p++] = (byte) (0x80 | (c & 0x3F));
            }
        }
        return p;
    }

    // ============ decimal 辅助 ============

    /** 编码 mantissa/exponent 为 SBE DecimalValue；exponent 溢出 int8 返回 false。 */
    private static boolean putDecimal(DecimalValueEncoder dv, long mantissa, int exponent) {
        if (exponent > 127 || exponent < -128) return false;
        dv.mantissa(mantissa).exponent((byte) exponent);
        return true;
    }
}
