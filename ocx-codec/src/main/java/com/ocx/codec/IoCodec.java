package com.ocx.codec;

import com.ocx.codec.domain.DepthItemVo;
import com.ocx.codec.domain.DepthLevel;
import com.ocx.codec.domain.DepthSnapshotMessage;
import com.ocx.codec.domain.ExchangeTrade;
import com.ocx.codec.domain.OrderIn;
import com.ocx.codec.domain.OrderInFields;
import com.ocx.codec.enums.OrderDirection;
import com.ocx.codec.sbe.CancelAckDecoder;
import com.ocx.codec.sbe.CancelAckEncoder;
import com.ocx.codec.sbe.DecimalValueDecoder;
import com.ocx.codec.sbe.DecimalValueEncoder;
import com.ocx.codec.sbe.DepthSnapshotDecoder;
import com.ocx.codec.sbe.DepthSnapshotEncoder;
import com.ocx.codec.sbe.DltRecordDecoder;
import com.ocx.codec.sbe.DltRecordEncoder;
import com.ocx.codec.sbe.ExceptionRecordDecoder;
import com.ocx.codec.sbe.ExceptionRecordEncoder;
import com.ocx.codec.sbe.ExchangeTradeDecoder;
import com.ocx.codec.sbe.ExchangeTradeEncoder;
import com.ocx.codec.sbe.MessageHeaderDecoder;
import com.ocx.codec.sbe.MessageHeaderEncoder;
import com.ocx.codec.sbe.OrderInDecoder;
import com.ocx.codec.sbe.OrderInEncoder;
import com.ocx.codec.sbe.OrderRejectDecoder;
import com.ocx.codec.sbe.OrderRejectEncoder;
import com.ocx.codec.sbe.VarStringDecoder;
import com.ocx.codec.sbe.VarStringEncoder;
import com.ocx.math.Decimal;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * 撮合引擎出入站二进制编解码（SBE）。
 *
 * <p>线上布局：4 字节魔数 {@code OCX2} + SBE messageHeader（blockLength/templateId/schemaId/version）
 * + 消息体。出入站仅使用 SBE，不再支持 JSON。</p>
 */
public final class IoCodec {

    /** 二进制格式魔数：OCX + 版本 2（出入站 wire 协议）。 */
    public static final byte[] MAGIC = {'O', 'C', 'X', '2'};

    public static final int MAGIC_LEN = MAGIC.length;

    private static final int MAX_ENCODED_LENGTH = 1024;

    /** 单笔成交 SBE 帧最大长度（含魔数），用于批量帧编码缓冲容量规划。 */
    public static final int MAX_TRADE_FRAME_LENGTH = MAGIC_LEN + MAX_ENCODED_LENGTH;

    /** 单条订单拒绝 SBE 帧最大长度（含魔数），用于批量帧编码缓冲容量规划。 */
    public static final int MAX_ORDER_REJECT_FRAME_LENGTH = MAGIC_LEN + MAX_ENCODED_LENGTH;

    /** 批量帧长度前缀字节数（uint32 大端）。 */
    public static final int FRAME_LENGTH_PREFIX = Integer.BYTES;

    /** 深度快照可能携带完整盘口，使用更大的固定编码缓冲。 */
    private static final int MAX_DEPTH_ENCODED_LENGTH = 1 << 18;

    /** 深度快照单帧最大长度（含魔数），供调用方规划编码缓冲。 */
    public static final int MAX_DEPTH_FRAME_LENGTH = MAGIC_LEN + MAX_DEPTH_ENCODED_LENGTH;

    /**
     * 编解码复用上下文：一次 {@code ThreadLocal.get()} 取到全部缓冲/编解码器。
     * <p>此前 13 个独立 ThreadLocal 挤在同一线程的 ThreadLocalMap 里，hash 碰撞后每次 get
     * 都要沿探测链查找（JFR 实测消费者解码路径 getEntryAfterMiss 283 采样）；合并后热路径
     * 每笔订单只 get 一次，探测链大幅缩短。</p>
     */
    public static final class CodecContext {
        public final byte[] tradeBuf = new byte[MAGIC_LEN + MAX_ENCODED_LENGTH];
        public final byte[] depthBuf = new byte[MAGIC_LEN + MAX_DEPTH_ENCODED_LENGTH];
        public final UnsafeBuffer tradeBuffer = new UnsafeBuffer(tradeBuf);
        public final UnsafeBuffer depthBuffer = new UnsafeBuffer(depthBuf);
        public final UnsafeBuffer decodeBuffer = new UnsafeBuffer(new byte[0]);
        public final SymbolBytes symbolCache = new SymbolBytes();
        public final OrderInDecoder orderInDecoder = new OrderInDecoder();
        public final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        public final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
        public final ExchangeTradeEncoder tradeEncoder = new ExchangeTradeEncoder();
        public final ExchangeTradeDecoder tradeDecoder = new ExchangeTradeDecoder();
        public final DepthSnapshotEncoder depthSnapshotEncoder = new DepthSnapshotEncoder();
        public final DepthSnapshotDecoder depthSnapshotDecoder = new DepthSnapshotDecoder();
        public final OrderRejectEncoder rejectEncoder = new OrderRejectEncoder();
        public final OrderRejectDecoder rejectDecoder = new OrderRejectDecoder();
        public final CancelAckEncoder cancelAckEncoder = new CancelAckEncoder();
        public final CancelAckDecoder cancelAckDecoder = new CancelAckDecoder();
        public final DltRecordEncoder dltRecordEncoder = new DltRecordEncoder();
        public final DltRecordDecoder dltRecordDecoder = new DltRecordDecoder();
        public final ExceptionRecordEncoder exceptionRecordEncoder = new ExceptionRecordEncoder();
        public final ExceptionRecordDecoder exceptionRecordDecoder = new ExceptionRecordDecoder();
    }

    private static final ThreadLocal<CodecContext> CTX = ThreadLocal.withInitial(CodecContext::new);

    private IoCodec() {}

    /** 获取当前线程的编解码复用上下文（撮合线程启动时缓存一次，热路径免每单 ThreadLocal.get）。 */
    public static CodecContext context() {
        return CTX.get();
    }

    public static boolean isSbe(byte[] payload) {
        return isSbe(payload, 0);
    }

    /** 偏移版魔数检查（批量帧内各帧从 offset 起）。 */
    public static boolean isSbe(byte[] payload, int offset) {
        if (payload == null || payload.length - offset < MAGIC_LEN) {
            return false;
        }
        for (int i = 0; i < MAGIC_LEN; i++) {
            if (payload[offset + i] != MAGIC[i]) {
                return false;
            }
        }
        return true;
    }

    public static boolean isSbe(ByteBuffer payload) {
        if (payload == null || payload.remaining() < MAGIC_LEN) {
            return false;
        }
        int pos = payload.position();
        for (int i = 0; i < MAGIC_LEN; i++) {
            if (payload.get(pos + i) != MAGIC[i]) {
                return false;
            }
        }
        return true;
    }

    /** Aeron 入站片段零拷贝魔数检查（DirectBuffer + 帧偏移 + 帧长）。 */
    public static boolean isSbe(DirectBuffer payload, int offset, int length) {
        if (payload == null || length < MAGIC_LEN) {
            return false;
        }
        for (int i = 0; i < MAGIC_LEN; i++) {
            if (payload.getByte(offset + i) != MAGIC[i]) {
                return false;
            }
        }
        return true;
    }

    // ==================== OrderIn ====================

    /** TIF 枚举编码（与撮合 Order.TIF_* 一致）：0=GTC 1=IOC 2=FOK 3=POST_ONLY。 */
    public static final byte TIF_GTC = 0;

    public static final byte TIF_IOC = 1;
    public static final byte TIF_FOK = 2;
    public static final byte TIF_POST_ONLY = 3;

    /** OrderIn 定长 symbol 字节数（SBE symbol16）。 */
    public static final int ORDER_IN_SYMBOL_LENGTH = 16;

    /** SBE OrderIn 帧最大长度（含魔数）。定长布局约 92 字节，256 为未来字段扩展留余量。 */
    public static final int MAX_ORDER_IN_FRAME_LENGTH = 256;

    /** 最小合法 OrderIn 帧长度：MAGIC + SBE header + 定长消息体。 */
    public static final int MIN_ORDER_IN_FRAME_LENGTH =
            MAGIC_LEN + MessageHeaderDecoder.ENCODED_LENGTH + OrderInDecoder.BLOCK_LENGTH;

    /** 帧内 symbol 编码偏移（含魔数）：MAGIC + header + id + userId。 */
    public static final int ORDER_IN_SYMBOL_OFFSET =
            MAGIC_LEN + MessageHeaderDecoder.ENCODED_LENGTH + OrderInDecoder.symbolEncodingOffset();

    /** symbol → 定长 16 字节 ASCII 键（右补零），供 SBE fixed string 直写/路由键。 */
    public static byte[] symbolKey(String symbol) {
        byte[] key = new byte[ORDER_IN_SYMBOL_LENGTH];
        byte[] src = symbol.getBytes(StandardCharsets.US_ASCII);
        if (src.length > ORDER_IN_SYMBOL_LENGTH) {
            throw new IllegalArgumentException("symbol 超过 16 字节: " + symbol);
        }
        System.arraycopy(src, 0, key, 0, src.length);
        return key;
    }

    /** 从入站缓冲按绝对偏移直读定长 symbol（不解析其他字段），供路由线程使用。 */
    public static void readOrderInSymbol(ByteBuffer payload, byte[] dst) {
        payload.get(ORDER_IN_SYMBOL_OFFSET, dst, 0, ORDER_IN_SYMBOL_LENGTH);
    }

    /** 从 Aeron 入站片段按绝对偏移直读定长 symbol（零拷贝）。offset 为帧起点（含魔数）。 */
    public static void readOrderInSymbol(DirectBuffer payload, int offset, byte[] dst) {
        payload.getBytes(offset + ORDER_IN_SYMBOL_OFFSET, dst, 0, ORDER_IN_SYMBOL_LENGTH);
    }

    public static byte[] encodeOrderIn(OrderIn o) {
        byte[] buf = new byte[MAGIC_LEN + MAX_ENCODED_LENGTH];
        int len = encodeOrderIn(o, buf, 0);
        return copyExact(buf, len);
    }

    /** 编码订单到给定缓冲（0 偏移），返回编码长度（调用方保证容量足够，发送完成前不得复用）。 */
    public static int encodeOrderIn(OrderIn o, byte[] buf) {
        return encodeOrderIn(o, buf, 0);
    }

    /** 编码订单到给定缓冲的指定偏移，返回编码长度。 */
    public static int encodeOrderIn(OrderIn o, byte[] buf, int offset) {
        MutableDirectBuffer b = CTX.get().tradeBuffer;
        b.wrap(buf, offset, buf.length - offset);
        b.putBytes(0, MAGIC);

        MessageHeaderEncoder header = CTX.get().headerEncoder;
        OrderInEncoder enc = new OrderInEncoder();
        enc.wrapAndApplyHeader(b, MAGIC_LEN, header);

        enc.id(o.id() == null ? 0L : o.id());
        enc.userId(o.userId() == null ? 0L : o.userId());
        enc.symbol(o.symbol() == null ? "" : o.symbol());
        writeDecimal(enc.price(), o.price());
        writeDecimal(enc.origQty(), o.origQty());
        writeDecimal(enc.executedQty(), o.executedQty());
        writeDecimal(enc.mergeLowPrice(), o.mergeLowPrice());

        enc.side(
                o.side() == null
                        ? com.ocx.codec.sbe.OrderDirection.BUY
                        : (o.side() == 1
                                ? com.ocx.codec.sbe.OrderDirection.SELL
                                : com.ocx.codec.sbe.OrderDirection.BUY));
        enc.typeCode(o.type() == null ? (short) 0 : o.type());
        enc.timeInForce(timeInForceCode(o.timeInForce()));
        enc.status(o.status() == null ? (short) 0 : o.status());
        enc.createTime(toEpochMillis(o.createTime()));
        writeDecimal(enc.visibleQty(), o.visibleQty());

        return MAGIC_LEN + MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
    }

    public static OrderIn decodeOrderIn(byte[] payload) {
        if (!isSbe(payload)) {
            throw new IllegalArgumentException("payload 不是 SBE 格式（缺少魔数 OCX2）");
        }
        CodecContext ctx = CTX.get();
        UnsafeBuffer b = ctx.decodeBuffer;
        b.wrap(payload);
        return decodeOrderIn(b);
    }

    public static OrderIn decodeOrderIn(ByteBuffer payload) {
        if (!isSbe(payload)) {
            throw new IllegalArgumentException("payload 不是 SBE 格式（缺少魔数 OCX2）");
        }
        CodecContext ctx = CTX.get();
        UnsafeBuffer b = ctx.decodeBuffer;
        b.wrap(payload);
        return decodeOrderIn(b);
    }

    /**
     * SBE OrderIn 帧字段直读（撮合线程热路径）：按偏移读出全部原生字段到
     * {@link OrderInFields}，零分配、零字符串、零 Decimal 对象。
     * 定长 symbol 不在此读取——handler 侧按实例绑定常量，无需逐单解析。
     */
    public static void readOrderIn(byte[] frame, int frameLen, OrderInFields out) {
        readOrderIn(CTX.get(), frame, frameLen, out);
    }

    /**
     * 上下文直传重载：撮合线程持有 {@link #context()} 缓存，热路径免 ThreadLocal.get。
     *
     * <p>信任入站已校验（消费线程 onOrderIn 已核对魔数/templateId/长度，事件槽只承载
     * 合法帧），跳过 templateId 重复检查——500K JFR：MessageHeaderDecoder.templateId
     * 读取占撮合线程 10.3% 采样。</p>
     */
    public static void readOrderIn(
            CodecContext ctx, byte[] frame, int frameLen, OrderInFields out) {
        UnsafeBuffer b = ctx.decodeBuffer;
        b.wrap(frame, 0, frameLen);
        decodeOrderInFields(ctx, b, 0, out);
    }

    /**
     * Aeron 入站片段零拷贝字段直读：offset 为帧起点（含魔数），length 为帧长。
     *
     * <p>直接让 SBE 解码器 wrap 原始帧缓冲——不再经 {@code decodeBuffer} 中转
     * （4.9M 裸跑 JFR：{@code UnsafeBuffer.wrap} 曾占撮合线程 15.7% 采样，
     * 每次入站都做 5 次接口调用 + 边界检查 + 5 次字段写，纯属浪费）。</p>
     */
    public static void readOrderIn(
            CodecContext ctx, DirectBuffer frame, int offset, int length, OrderInFields out) {
        decodeOrderInFields(ctx, frame, offset, out);
    }

    /** OrderIn.status：2=撤单（仅消费 id）、99=批量撤单（仅消费 userId）。 */
    public static final byte STATUS_CANCEL = 2;

    public static final byte STATUS_BATCH_CANCEL = 99;

    /**
     * OrderIn SBE 字段偏移（相对 SBE 消息起点；schema 定长 BLOCK_LENGTH=89，
     * 布局见生成 {@link OrderInDecoder}）。撮合线程按偏移直读，零 accessor/Decimal wrap。
     *
     * <p>版本纪律：schema 只允许尾部追加字段（见 ocx-io-schema.xml 注释），本常量
     * 与生成布局的对拍由 {@code OrderInDirectReadLayoutTest} 钉死——中段插字段导致
     * 偏移漂移时该测试显式失败。</p>
     */
    private static final int F_ID = 0;

    private static final int F_USER_ID = 8;
    private static final int F_PRICE_MANTISSA = 32;
    private static final int F_PRICE_EXPONENT = 40;
    private static final int F_ORIG_QTY_MANTISSA = 41;
    private static final int F_ORIG_QTY_EXPONENT = 49;
    private static final int F_EXEC_QTY_MANTISSA = 50;
    private static final int F_EXEC_QTY_EXPONENT = 58;
    private static final int F_MERGE_LOW_PRICE_MANTISSA = 59;
    private static final int F_MERGE_LOW_PRICE_EXPONENT = 67;
    private static final int F_SIDE = 68;
    private static final int F_TYPE = 69;
    private static final int F_TIME_IN_FORCE = 70;
    private static final int F_STATUS = 71;
    private static final int F_CREATE_TIME = 72;
    private static final int F_VISIBLE_QTY_MANTISSA = 80;
    private static final int F_VISIBLE_QTY_EXPONENT = 88;

    /**
     * 按类型解码（2026-08-29）：先读 status（偏移 71）决定字段集——撤单/批撤只解
     * id+userId，其余订单字段裁剪为零（防槽位残留误判）；普通/条件单静态偏移直读
     * 全字段。消灭 Decimal wrap 与未内联 accessor（JFR：OrderInDecoder.id 叶子曾占
     * 撮合线程 37.5%，即整个 SBE 解码段的归属）。
     *
     * <p>信任边界沿用：消费线程已校验魔数/templateId/长度，schema 定长，撮合线程
     * 直接按常量偏移读，无逐字段边界检查。</p>
     */
    private static void decodeOrderInFields(
            CodecContext ctx, DirectBuffer b, int baseOffset, OrderInFields out) {
        int s = baseOffset + MAGIC_LEN + MessageHeaderDecoder.ENCODED_LENGTH;
        int status = b.getByte(s + F_STATUS) & 0xFF;
        out.status = (byte) status;
        out.id = b.getLong(s + F_ID, OrderInDecoder.BYTE_ORDER);
        out.userId = b.getLong(s + F_USER_ID, OrderInDecoder.BYTE_ORDER);
        if (status == STATUS_BATCH_CANCEL || status == STATUS_CANCEL) {
            out.priceMantissa = 0L;
            out.priceExponent = 0;
            out.origQtyMantissa = 0L;
            out.origQtyExponent = 0;
            out.executedQtyMantissa = 0L;
            out.executedQtyExponent = 0;
            out.mergeLowPriceMantissa = 0L;
            out.mergeLowPriceExponent = 0;
            out.side = 0;
            out.type = 0;
            out.timeInForce = 0;
            out.createTimeMillis = 0L;
            out.visibleQtyMantissa = 0L;
            out.visibleQtyExponent = 0;
            return;
        }
        out.priceMantissa = b.getLong(s + F_PRICE_MANTISSA, OrderInDecoder.BYTE_ORDER);
        out.priceExponent = b.getByte(s + F_PRICE_EXPONENT);
        out.origQtyMantissa = b.getLong(s + F_ORIG_QTY_MANTISSA, OrderInDecoder.BYTE_ORDER);
        out.origQtyExponent = b.getByte(s + F_ORIG_QTY_EXPONENT);
        out.executedQtyMantissa = b.getLong(s + F_EXEC_QTY_MANTISSA, OrderInDecoder.BYTE_ORDER);
        out.executedQtyExponent = b.getByte(s + F_EXEC_QTY_EXPONENT);
        out.mergeLowPriceMantissa =
                b.getLong(s + F_MERGE_LOW_PRICE_MANTISSA, OrderInDecoder.BYTE_ORDER);
        out.mergeLowPriceExponent = b.getByte(s + F_MERGE_LOW_PRICE_EXPONENT);
        out.side = (byte) (b.getByte(s + F_SIDE) & 0xFF);
        out.type = (byte) (b.getByte(s + F_TYPE) & 0xFF);
        out.timeInForce = (byte) (b.getByte(s + F_TIME_IN_FORCE) & 0xFF);
        out.createTimeMillis = b.getLong(s + F_CREATE_TIME, OrderInDecoder.BYTE_ORDER);
        out.visibleQtyMantissa = b.getLong(s + F_VISIBLE_QTY_MANTISSA, OrderInDecoder.BYTE_ORDER);
        out.visibleQtyExponent = b.getByte(s + F_VISIBLE_QTY_EXPONENT);
    }

    private static OrderIn decodeOrderIn(DirectBuffer b) {
        MessageHeaderDecoder header = CTX.get().headerDecoder;
        header.wrap(b, MAGIC_LEN);
        if (header.templateId() != OrderInDecoder.TEMPLATE_ID) {
            throw new IllegalArgumentException("未知 SBE templateId: " + header.templateId());
        }
        OrderInDecoder dec = CTX.get().orderInDecoder;
        dec.wrap(
                b,
                MAGIC_LEN + MessageHeaderDecoder.ENCODED_LENGTH,
                header.blockLength(),
                header.version());

        // userId=0 是合法值（market-maker 合成单），不得映射为 null，否则入站校验会拒绝
        Long userId = dec.userId();
        String symbol = dec.symbol();
        Decimal price = dec.price().mantissa() == 0 ? null : readDecimal(dec.price());
        Decimal origQty = readDecimal(dec.origQty());
        // executedQty=0 是合法业务值（未成交），数值上等价于缺省，统一解码为 Decimal.ZERO
        Decimal executedQty =
                dec.executedQty().mantissa() == 0 ? Decimal.ZERO : readDecimal(dec.executedQty());
        Decimal mergeLowPrice =
                dec.mergeLowPrice().mantissa() == 0 ? null : readDecimal(dec.mergeLowPrice());
        Decimal visibleQty =
                dec.visibleQty().mantissa() == 0 ? null : readDecimal(dec.visibleQty());

        Byte side = (byte) dec.side().value();
        Byte type = dec.typeCode() == 0 ? null : (byte) dec.typeCode();
        // status=0 是合法业务值（新订单），不得映射为 null
        Byte status = (byte) dec.status();

        return new OrderIn(
                dec.id() == 0 ? null : dec.id(),
                userId,
                symbol,
                price,
                origQty,
                executedQty,
                mergeLowPrice,
                side,
                type,
                timeInForceName((byte) dec.timeInForce()),
                status,
                fromEpochMillis(dec.createTime()),
                visibleQty);
    }

    /** TIF 字符串 → SBE uint8 枚举 code（未知/缺省 → GTC）。 */
    private static byte timeInForceCode(String tif) {
        if (tif == null) {
            return TIF_GTC;
        }
        return switch (tif) {
            case "IOC" -> TIF_IOC;
            case "FOK" -> TIF_FOK;
            case "POST_ONLY" -> TIF_POST_ONLY;
            default -> TIF_GTC;
        };
    }

    /** SBE uint8 枚举 code → TIF 字符串（边界/测试路径用）。 */
    private static String timeInForceName(byte code) {
        return switch (code) {
            case TIF_IOC -> "IOC";
            case TIF_FOK -> "FOK";
            case TIF_POST_ONLY -> "POST_ONLY";
            default -> "GTC";
        };
    }

    private static long toEpochMillis(LocalDateTime t) {
        return t == null
                ? 0L
                : t.atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    private static LocalDateTime fromEpochMillis(long millis) {
        return millis == 0L
                ? null
                : LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(millis), java.time.ZoneId.systemDefault());
    }

    // ==================== ExchangeTrade ====================

    public static byte[] encodeExchangeTrade(ExchangeTrade t) {
        byte[] buf = CTX.get().tradeBuf;
        int len = encodeExchangeTrade(t, buf, 0);
        return copyExact(buf, len);
    }

    /** 编码成交到给定缓冲（从 0 偏移），返回编码长度（调用方保证容量足够，且发送完成前不得复用）。 */
    public static int encodeExchangeTrade(ExchangeTrade t, byte[] buf) {
        return encodeExchangeTrade(t, buf, 0);
    }

    /** 编码成交到给定缓冲的指定偏移，返回编码长度。 */
    public static int encodeExchangeTrade(ExchangeTrade t, byte[] buf, int offset) {
        MutableDirectBuffer b = CTX.get().tradeBuffer;
        b.wrap(buf, offset, buf.length - offset);
        b.putBytes(0, MAGIC);

        MessageHeaderEncoder header = CTX.get().headerEncoder;
        ExchangeTradeEncoder enc = CTX.get().tradeEncoder;
        enc.wrapAndApplyHeader(b, MAGIC_LEN, header);

        enc.symbol(t.getSymbol());

        enc.direction(toSbe(t.getDirection()));
        writeDecimal(enc.price(), t.getPriceMantissa(), t.getPriceExponent());
        writeDecimal(enc.amount(), t.getAmountMantissa(), t.getAmountExponent());

        enc.buyOrderId(t.getBuyOrderId());
        enc.sellOrderId(t.getSellOrderId());

        writeDecimal(enc.buyTurnover(), t.getBuyTurnoverMantissa(), t.getBuyTurnoverExponent());
        writeDecimal(enc.sellTurnover(), t.getSellTurnoverMantissa(), t.getSellTurnoverExponent());
        enc.time(t.getTime());

        return MAGIC_LEN + MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
    }

    /**
     * 字段版成交编码（纵表链路，零 ExchangeTrade 对象）— 与
     * {@link #encodeExchangeTrade(ExchangeTrade, byte[], int)} 同布局。
     *
     * @param direction 0=BUY 1=SELL（对齐 OrderDirection 数值约定）
     */
    public static int encodeTradeFields(
            String symbol,
            int direction,
            long priceMantissa,
            int priceExponent,
            long amountMantissa,
            int amountExponent,
            long buyOrderId,
            long sellOrderId,
            long buyTurnoverMantissa,
            int buyTurnoverExponent,
            long sellTurnoverMantissa,
            int sellTurnoverExponent,
            long time,
            byte[] buf,
            int offset) {
        return encodeTradeFields(
                CTX.get(),
                symbol,
                direction,
                priceMantissa,
                priceExponent,
                amountMantissa,
                amountExponent,
                buyOrderId,
                sellOrderId,
                buyTurnoverMantissa,
                buyTurnoverExponent,
                sellTurnoverMantissa,
                sellTurnoverExponent,
                time,
                buf,
                offset);
    }

    /**
     * 上下文直传版（egress 发送线程热路径）：调用方在单写线程缓存
     * {@link #context()}，免每笔成交 3 次 ThreadLocal.get（JFR 实测 ~5% 采样）。
     */
    public static int encodeTradeFields(
            CodecContext ctx,
            String symbol,
            int direction,
            long priceMantissa,
            int priceExponent,
            long amountMantissa,
            int amountExponent,
            long buyOrderId,
            long sellOrderId,
            long buyTurnoverMantissa,
            int buyTurnoverExponent,
            long sellTurnoverMantissa,
            int sellTurnoverExponent,
            long time,
            byte[] buf,
            int offset) {
        MutableDirectBuffer b = ctx.tradeBuffer;
        b.wrap(buf, offset, buf.length - offset);
        b.putBytes(0, MAGIC);

        MessageHeaderEncoder header = ctx.headerEncoder;
        ExchangeTradeEncoder enc = ctx.tradeEncoder;
        enc.wrapAndApplyHeader(b, MAGIC_LEN, header);

        enc.symbol(symbol);

        enc.direction(
                direction == 1
                        ? com.ocx.codec.sbe.OrderDirection.SELL
                        : com.ocx.codec.sbe.OrderDirection.BUY);
        writeDecimal(enc.price(), priceMantissa, priceExponent);
        writeDecimal(enc.amount(), amountMantissa, amountExponent);

        enc.buyOrderId(buyOrderId);
        enc.sellOrderId(sellOrderId);

        writeDecimal(enc.buyTurnover(), buyTurnoverMantissa, buyTurnoverExponent);
        writeDecimal(enc.sellTurnover(), sellTurnoverMantissa, sellTurnoverExponent);
        enc.time(time);

        return MAGIC_LEN + MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
    }

    /**
     * 定长 symbol 字节版（egress 发送线程热路径）：symbol 预编码为 16 字节
     * ASCII 键，走 {@code putSymbol} 免每笔 String→UTF-8（JFR 实测 ~4.5% 采样）。
     * 与 String 版输出字节完全一致（SBE fixed string 定长 16）。
     */
    public static int encodeTradeFields(
            CodecContext ctx,
            byte[] symbol16,
            int direction,
            long priceMantissa,
            int priceExponent,
            long amountMantissa,
            int amountExponent,
            long buyOrderId,
            long sellOrderId,
            long buyTurnoverMantissa,
            int buyTurnoverExponent,
            long sellTurnoverMantissa,
            int sellTurnoverExponent,
            long time,
            byte[] buf,
            int offset) {
        MutableDirectBuffer b = ctx.tradeBuffer;
        b.wrap(buf, offset, buf.length - offset);
        b.putBytes(0, MAGIC);

        MessageHeaderEncoder header = ctx.headerEncoder;
        ExchangeTradeEncoder enc = ctx.tradeEncoder;
        enc.wrapAndApplyHeader(b, MAGIC_LEN, header);

        enc.putSymbol(symbol16, 0);

        enc.direction(
                direction == 1
                        ? com.ocx.codec.sbe.OrderDirection.SELL
                        : com.ocx.codec.sbe.OrderDirection.BUY);
        writeDecimal(enc.price(), priceMantissa, priceExponent);
        writeDecimal(enc.amount(), amountMantissa, amountExponent);

        enc.buyOrderId(buyOrderId);
        enc.sellOrderId(sellOrderId);

        writeDecimal(enc.buyTurnover(), buyTurnoverMantissa, buyTurnoverExponent);
        writeDecimal(enc.sellTurnover(), sellTurnoverMantissa, sellTurnoverExponent);
        enc.time(time);

        return MAGIC_LEN + MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
    }

    /**
     * 批量编码成交到给定缓冲：{@code [uint32 BE 帧长][SBE 帧]} 连续拼接，返回总长度。
     * 调用方保证容量足够（{@code trades.size() * (FRAME_LENGTH_PREFIX + MAX_TRADE_FRAME_LENGTH)}），
     * 且发送完成前不得复用缓冲。
     */
    public static int encodeExchangeTradeBatch(List<ExchangeTrade> trades, byte[] buf) {
        int pos = 0;
        for (ExchangeTrade t : trades) {
            int len = encodeExchangeTrade(t, buf, pos + FRAME_LENGTH_PREFIX);
            writeIntBE(buf, pos, len);
            pos += FRAME_LENGTH_PREFIX + len;
        }
        return pos;
    }

    public static ExchangeTrade decodeExchangeTrade(byte[] payload) {
        return decodeExchangeTrade(payload, 0);
    }

    /** 偏移版单帧解码（批量帧内各帧从 offset 起）。 */
    public static ExchangeTrade decodeExchangeTrade(byte[] payload, int offset) {
        if (!isSbe(payload, offset)) {
            throw new IllegalArgumentException("payload 不是 SBE 格式（缺少魔数 OCX2）");
        }
        UnsafeBuffer b = CTX.get().decodeBuffer;
        b.wrap(payload, offset, payload.length - offset);
        MessageHeaderDecoder header = CTX.get().headerDecoder;
        header.wrap(b, MAGIC_LEN);
        if (header.templateId() != ExchangeTradeDecoder.TEMPLATE_ID) {
            throw new IllegalArgumentException("未知 SBE templateId: " + header.templateId());
        }

        ExchangeTradeDecoder dec = CTX.get().tradeDecoder;
        dec.wrap(
                b,
                MAGIC_LEN + MessageHeaderDecoder.ENCODED_LENGTH,
                header.blockLength(),
                header.version());

        ExchangeTrade t = new ExchangeTrade();
        t.setSymbol(dec.symbol());

        t.setDirection(fromSbe(dec.direction()));
        t.setPrice(dec.price().mantissa(), dec.price().exponent());
        t.setAmount(dec.amount().mantissa(), dec.amount().exponent());

        t.setBuyOrderId(dec.buyOrderId());
        t.setSellOrderId(dec.sellOrderId());

        t.setBuyTurnover(dec.buyTurnover().mantissa(), dec.buyTurnover().exponent());
        t.setSellTurnover(dec.sellTurnover().mantissa(), dec.sellTurnover().exponent());
        t.setTime(dec.time());
        return t;
    }

    /**
     * 兼容解析一条消息中的成交：旧单帧（首 4 字节 == MAGIC）直接解析一笔；
     * 否则按批量帧 {@code [uint32 BE 帧长][SBE 帧]} 循环解析。逐笔回调，不产生中间列表。
     */
    public static void forEachExchangeTrade(
            byte[] payload, java.util.function.Consumer<ExchangeTrade> consumer) {
        if (payload == null || payload.length == 0) {
            return;
        }
        if (isSbe(payload, 0)) {
            ExchangeTrade t = decodeExchangeTrade(payload, 0);
            if (t != null) {
                consumer.accept(t);
            }
            return;
        }
        int offset = 0;
        int total = payload.length;
        while (offset < total) {
            if (total - offset < FRAME_LENGTH_PREFIX) {
                throw new IllegalArgumentException("批量帧长度前缀不完整: remaining=" + (total - offset));
            }
            int len = readIntBE(payload, offset);
            offset += FRAME_LENGTH_PREFIX;
            if (len <= 0 || offset + len > total) {
                throw new IllegalArgumentException(
                        "批量帧长度越界: len=" + len + " remaining=" + (total - offset));
            }
            ExchangeTrade t = decodeExchangeTrade(payload, offset);
            if (t != null) {
                consumer.accept(t);
            }
            offset += len;
        }
    }

    /** 统计一条消息内的成交笔数（兼容旧单帧与批量帧），供测试/消费端指标使用。 */
    public static int countExchangeTrades(byte[] payload) {
        int[] count = {0};
        forEachExchangeTrade(payload, t -> count[0]++);
        return count[0];
    }

    // ==================== DepthSnapshot ====================

    /** 盘口深度全量快照编码：symbol + ts + bids/asks 完整档位。 */
    public static byte[] encodeDepthSnapshot(
            String symbol, long ts, List<DepthItemVo> asks, List<DepthItemVo> bids) {
        byte[] buf = CTX.get().depthBuf;
        MutableDirectBuffer b = new UnsafeBuffer(buf);
        b.putBytes(0, MAGIC);

        MessageHeaderEncoder header = CTX.get().headerEncoder;
        DepthSnapshotEncoder enc = CTX.get().depthSnapshotEncoder;
        enc.wrapAndApplyHeader(b, MAGIC_LEN, header);

        enc.symbol(symbol);
        enc.ts(ts);

        DepthSnapshotEncoder.AsksEncoder asksEnc = enc.asksCount(asks.size());
        for (DepthItemVo level : asks) {
            asksEnc.next();
            writeDecimal(asksEnc.price(), level.price());
            writeDecimal(asksEnc.volume(), level.volume());
        }

        DepthSnapshotEncoder.BidsEncoder bidsEnc = enc.bidsCount(bids.size());
        for (DepthItemVo level : bids) {
            bidsEnc.next();
            writeDecimal(bidsEnc.price(), level.price());
            writeDecimal(bidsEnc.volume(), level.volume());
        }

        return copyExact(
                buf, MAGIC_LEN + MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength());
    }

    /** 盘口深度全量快照编码：档位为原始 long（价格/数量已按 symbol 固定精度归一化）。 */
    public static byte[] encodeDepthSnapshot(
            String symbol,
            long ts,
            int priceScale,
            int volumeScale,
            List<DepthLevel> asks,
            List<DepthLevel> bids) {
        byte[] buf = CTX.get().depthBuf;
        MutableDirectBuffer b = new UnsafeBuffer(buf);
        b.putBytes(0, MAGIC);

        MessageHeaderEncoder header = CTX.get().headerEncoder;
        DepthSnapshotEncoder enc = CTX.get().depthSnapshotEncoder;
        enc.wrapAndApplyHeader(b, MAGIC_LEN, header);

        enc.symbol(symbol);
        enc.ts(ts);

        DepthSnapshotEncoder.AsksEncoder asksEnc = enc.asksCount(asks.size());
        for (DepthLevel level : asks) {
            asksEnc.next();
            writeDecimal(asksEnc.price(), level.price(), -priceScale);
            writeDecimal(asksEnc.volume(), level.volume(), -volumeScale);
        }

        DepthSnapshotEncoder.BidsEncoder bidsEnc = enc.bidsCount(bids.size());
        for (DepthLevel level : bids) {
            bidsEnc.next();
            writeDecimal(bidsEnc.price(), level.price(), -priceScale);
            writeDecimal(bidsEnc.volume(), level.volume(), -volumeScale);
        }

        return copyExact(
                buf, MAGIC_LEN + MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength());
    }

    /** 盘口深度全量快照编码：档位为预分配的 long 数组，避免 List/DepthLevel 分配。 */
    public static byte[] encodeDepthSnapshot(
            String symbol,
            long ts,
            int priceScale,
            int volumeScale,
            long[] askPrices,
            long[] askVolumes,
            int askCount,
            long[] bidPrices,
            long[] bidVolumes,
            int bidCount) {
        byte[] buf = CTX.get().depthBuf;
        int len =
                encodeDepthSnapshot(
                        symbol,
                        ts,
                        priceScale,
                        volumeScale,
                        askPrices,
                        askVolumes,
                        askCount,
                        bidPrices,
                        bidVolumes,
                        bidCount,
                        buf);
        return copyExact(buf, len);
    }

    /** 编码深度快照到给定缓冲，返回编码长度（调用方保证容量足够，且发送完成前不得复用）。 */
    public static int encodeDepthSnapshot(
            String symbol,
            long ts,
            int priceScale,
            int volumeScale,
            long[] askPrices,
            long[] askVolumes,
            int askCount,
            long[] bidPrices,
            long[] bidVolumes,
            int bidCount,
            byte[] buf) {
        MutableDirectBuffer b = CTX.get().depthBuffer;
        b.wrap(buf);
        b.putBytes(0, MAGIC);

        MessageHeaderEncoder header = CTX.get().headerEncoder;
        DepthSnapshotEncoder enc = CTX.get().depthSnapshotEncoder;
        enc.wrapAndApplyHeader(b, MAGIC_LEN, header);

        enc.symbol(symbol);
        enc.ts(ts);

        DepthSnapshotEncoder.AsksEncoder asksEnc = enc.asksCount(askCount);
        for (int i = 0; i < askCount; i++) {
            asksEnc.next();
            writeDecimal(asksEnc.price(), askPrices[i], -priceScale);
            writeDecimal(asksEnc.volume(), askVolumes[i], -volumeScale);
        }

        DepthSnapshotEncoder.BidsEncoder bidsEnc = enc.bidsCount(bidCount);
        for (int i = 0; i < bidCount; i++) {
            bidsEnc.next();
            writeDecimal(bidsEnc.price(), bidPrices[i], -priceScale);
            writeDecimal(bidsEnc.volume(), bidVolumes[i], -volumeScale);
        }

        return MAGIC_LEN + MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
    }

    /** 盘口深度全量快照解码，还原为 {@link DepthSnapshotMessage}。 */
    public static DepthSnapshotMessage decodeDepthSnapshot(byte[] payload) {
        if (!isSbe(payload)) {
            throw new IllegalArgumentException("payload 不是 SBE 格式（缺少魔数 OCX2）");
        }
        UnsafeBuffer b = CTX.get().decodeBuffer;
        b.wrap(payload);
        MessageHeaderDecoder header = CTX.get().headerDecoder;
        header.wrap(b, MAGIC_LEN);
        if (header.templateId() != DepthSnapshotDecoder.TEMPLATE_ID) {
            throw new IllegalArgumentException("未知 SBE templateId: " + header.templateId());
        }

        DepthSnapshotDecoder dec = CTX.get().depthSnapshotDecoder;
        dec.wrap(
                b,
                MAGIC_LEN + MessageHeaderDecoder.ENCODED_LENGTH,
                header.blockLength(),
                header.version());

        String symbol = dec.symbol();

        List<DepthItemVo> asks = new ArrayList<>();
        for (DepthSnapshotDecoder.AsksDecoder a : dec.asks()) {
            asks.add(new DepthItemVo(readDecimal(a.price()), readDecimal(a.volume())));
        }
        List<DepthItemVo> bids = new ArrayList<>();
        for (DepthSnapshotDecoder.BidsDecoder bd : dec.bids()) {
            bids.add(new DepthItemVo(readDecimal(bd.price()), readDecimal(bd.volume())));
        }

        return new DepthSnapshotMessage(symbol, asks, bids);
    }

    // ==================== CancelAck ====================

    /** 撤单回执编码：单字段 orderId。 */
    public static byte[] encodeCancelAck(long orderId) {
        byte[] buf = new byte[MAGIC_LEN + MAX_ENCODED_LENGTH];
        MutableDirectBuffer b = new UnsafeBuffer(buf);
        b.putBytes(0, MAGIC);

        MessageHeaderEncoder header = CTX.get().headerEncoder;
        CancelAckEncoder enc = CTX.get().cancelAckEncoder;
        enc.wrapAndApplyHeader(b, MAGIC_LEN, header);

        int pos = enc.limit();
        enc.orderId(orderId);

        return copyExact(
                buf, MAGIC_LEN + MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength());
    }

    /** 撤单回执解码，返回 orderId。 */
    public static long decodeCancelAck(byte[] payload) {
        if (!isSbe(payload)) {
            throw new IllegalArgumentException("payload 不是 SBE 格式（缺少魔数 OCX2）");
        }
        UnsafeBuffer b = new UnsafeBuffer(payload);
        MessageHeaderDecoder header = CTX.get().headerDecoder;
        header.wrap(b, MAGIC_LEN);
        if (header.templateId() != CancelAckDecoder.TEMPLATE_ID) {
            throw new IllegalArgumentException("未知 SBE templateId: " + header.templateId());
        }

        CancelAckDecoder dec = CTX.get().cancelAckDecoder;
        dec.wrap(
                b,
                MAGIC_LEN + MessageHeaderDecoder.ENCODED_LENGTH,
                header.blockLength(),
                header.version());
        return dec.orderId();
    }

    // ==================== DltRecord / ExceptionRecord ====================

    /** 死信记录（运维排查用）。 */
    public record DltData(String reason, String raw) {}

    /** 撮合异常记录（运维排查用）。 */
    public record ExceptionData(long sequence, String message) {}

    public static byte[] encodeDltRecord(String reason, String raw) {
        byte[] buf = new byte[MAGIC_LEN + MAX_ENCODED_LENGTH];
        MutableDirectBuffer b = new UnsafeBuffer(buf);
        b.putBytes(0, MAGIC);

        MessageHeaderEncoder header = CTX.get().headerEncoder;
        DltRecordEncoder enc = CTX.get().dltRecordEncoder;
        enc.wrapAndApplyHeader(b, MAGIC_LEN, header);

        int pos = enc.limit();
        pos += writeVar(b, pos, reason, enc.reason());
        enc.limit(pos);
        pos = enc.limit();
        pos += writeVar(b, pos, raw, enc.raw());
        enc.limit(pos);

        return copyExact(
                buf, MAGIC_LEN + MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength());
    }

    public static DltData decodeDltRecord(byte[] payload) {
        if (!isSbe(payload)) {
            throw new IllegalArgumentException("payload 不是 SBE 格式（缺少魔数 OCX2）");
        }
        UnsafeBuffer b = new UnsafeBuffer(payload);
        MessageHeaderDecoder header = CTX.get().headerDecoder;
        header.wrap(b, MAGIC_LEN);
        if (header.templateId() != DltRecordDecoder.TEMPLATE_ID) {
            throw new IllegalArgumentException("未知 SBE templateId: " + header.templateId());
        }
        DltRecordDecoder dec = CTX.get().dltRecordDecoder;
        dec.wrap(
                b,
                MAGIC_LEN + MessageHeaderDecoder.ENCODED_LENGTH,
                header.blockLength(),
                header.version());
        int pos = dec.limit();
        String reason = readVar(b, pos, dec.reason());
        pos += dec.reason().length();
        String raw = readVar(b, pos, dec.raw());
        return new DltData(reason, raw);
    }

    public static byte[] encodeExceptionRecord(long sequence, String message) {
        byte[] buf = new byte[MAGIC_LEN + MAX_ENCODED_LENGTH];
        MutableDirectBuffer b = new UnsafeBuffer(buf);
        b.putBytes(0, MAGIC);

        MessageHeaderEncoder header = CTX.get().headerEncoder;
        ExceptionRecordEncoder enc = CTX.get().exceptionRecordEncoder;
        enc.wrapAndApplyHeader(b, MAGIC_LEN, header);
        enc.sequence(sequence);

        int pos = enc.limit();
        pos += writeVar(b, pos, message, enc.message());
        enc.limit(pos);

        return copyExact(
                buf, MAGIC_LEN + MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength());
    }

    public static ExceptionData decodeExceptionRecord(byte[] payload) {
        if (!isSbe(payload)) {
            throw new IllegalArgumentException("payload 不是 SBE 格式（缺少魔数 OCX2）");
        }
        UnsafeBuffer b = new UnsafeBuffer(payload);
        MessageHeaderDecoder header = CTX.get().headerDecoder;
        header.wrap(b, MAGIC_LEN);
        if (header.templateId() != ExceptionRecordDecoder.TEMPLATE_ID) {
            throw new IllegalArgumentException("未知 SBE templateId: " + header.templateId());
        }
        ExceptionRecordDecoder dec = CTX.get().exceptionRecordDecoder;
        dec.wrap(
                b,
                MAGIC_LEN + MessageHeaderDecoder.ENCODED_LENGTH,
                header.blockLength(),
                header.version());
        return new ExceptionData(dec.sequence(), readVar(b, dec.limit(), dec.message()));
    }

    // ==================== OrderReject ====================

    /**
     * 订单拒绝记录（撮合引擎出站，业务拒绝事件）。
     *
     * <p>price/remainingQty 用原生 mantissa/exponent（不再包 Decimal）：发送线程
     * drainRejects 每单省 2 次 {@code Decimal.of} 分配（500K 满簿 reject 洪峰下
     * JFR 分配 top Decimal.of 3.7% 的主要来源），编码时直接写 SBE decimalValue。</p>
     */
    public record OrderRejectData(
            long orderId,
            long userId,
            String symbol,
            String reason,
            long remainingQtyMantissa,
            int remainingQtyExponent,
            boolean pricePresent,
            long priceMantissa,
            int priceExponent,
            long ts) {}

    /** 订单拒绝编码：orderId/userId/ts + symbol/reason 变长 + remainingQty/price（decimal）。 */
    public static byte[] encodeOrderReject(OrderRejectData data) {
        byte[] buf = new byte[MAX_ORDER_REJECT_FRAME_LENGTH];
        int len = encodeOrderRejectFrame(data, buf, 0);
        return copyExact(buf, len);
    }

    /** 订单拒绝单帧编码到给定缓冲（批量帧内偏移），返回帧长（含魔数+header）。 */
    public static int encodeOrderRejectFrame(OrderRejectData data, byte[] buf, int offset) {
        UnsafeBuffer b = new UnsafeBuffer(buf, offset, buf.length - offset);
        b.putBytes(0, MAGIC);

        MessageHeaderEncoder header = CTX.get().headerEncoder;
        OrderRejectEncoder enc = CTX.get().rejectEncoder;
        enc.wrapAndApplyHeader(b, MAGIC_LEN, header);

        enc.orderId(data.orderId());
        enc.userId(data.userId());

        enc.symbol(data.symbol());
        int pos = enc.limit();
        pos += writeVar(b, pos, data.reason(), enc.reason());
        enc.limit(pos);

        writeDecimal(enc.remainingQty(), data.remainingQtyMantissa(), data.remainingQtyExponent());
        enc.pricePresent(data.pricePresent() ? (short) 1 : (short) 0);
        writeDecimal(enc.price(), data.priceMantissa(), data.priceExponent());
        enc.ts(data.ts());

        return MAGIC_LEN + MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
    }

    /**
     * 批量编码订单拒绝到给定缓冲：{@code [uint32 BE 帧长][SBE 帧]} 连续拼接，返回总长度。
     * 调用方保证容量足够（{@code rejects.size() * (FRAME_LENGTH_PREFIX + MAX_ORDER_REJECT_FRAME_LENGTH)}）。
     */
    public static int encodeOrderRejectBatch(List<OrderRejectData> rejects, byte[] buf) {
        int pos = 0;
        for (OrderRejectData d : rejects) {
            int len = encodeOrderRejectFrame(d, buf, pos + FRAME_LENGTH_PREFIX);
            writeIntBE(buf, pos, len);
            pos += FRAME_LENGTH_PREFIX + len;
        }
        return pos;
    }

    /**
     * 兼容解析一条消息中的订单拒绝：旧单帧（首 4 字节 == MAGIC）直接解析一条；
     * 否则按批量帧 {@code [uint32 BE 帧长][SBE 帧]} 循环解析。逐条回调，不产生中间列表。
     */
    public static void forEachOrderReject(
            byte[] payload, java.util.function.Consumer<OrderRejectData> consumer) {
        if (payload == null || payload.length == 0) {
            return;
        }
        if (isSbe(payload, 0)) {
            OrderRejectData d = decodeOrderReject(payload, 0);
            if (d != null) {
                consumer.accept(d);
            }
            return;
        }
        int offset = 0;
        int total = payload.length;
        while (offset < total) {
            if (total - offset < FRAME_LENGTH_PREFIX) {
                throw new IllegalArgumentException("批量帧长度前缀不完整: remaining=" + (total - offset));
            }
            int len = readIntBE(payload, offset);
            offset += FRAME_LENGTH_PREFIX;
            if (len <= 0 || offset + len > total) {
                throw new IllegalArgumentException(
                        "批量帧长度越界: len=" + len + " remaining=" + (total - offset));
            }
            OrderRejectData d = decodeOrderReject(payload, offset);
            if (d != null) {
                consumer.accept(d);
            }
            offset += len;
        }
    }

    /** 统计一条消息内的订单拒绝条数（兼容旧单帧与批量帧），供测试/消费端指标使用。 */
    public static int countOrderRejects(byte[] payload) {
        int[] count = {0};
        forEachOrderReject(payload, d -> count[0]++);
        return count[0];
    }

    /** 订单拒绝解码。 */
    public static OrderRejectData decodeOrderReject(byte[] payload) {
        return decodeOrderReject(payload, 0);
    }

    /** 偏移版单帧解码（批量帧内各帧从 offset 起）。 */
    public static OrderRejectData decodeOrderReject(byte[] payload, int offset) {
        if (!isSbe(payload, offset)) {
            throw new IllegalArgumentException("payload 不是 SBE 格式（缺少魔数 OCX2）");
        }
        UnsafeBuffer b = new UnsafeBuffer(payload, offset, payload.length - offset);
        MessageHeaderDecoder header = CTX.get().headerDecoder;
        header.wrap(b, MAGIC_LEN);
        if (header.templateId() != OrderRejectDecoder.TEMPLATE_ID) {
            throw new IllegalArgumentException("未知 SBE templateId: " + header.templateId());
        }
        OrderRejectDecoder dec = CTX.get().rejectDecoder;
        dec.wrap(
                b,
                MAGIC_LEN + MessageHeaderDecoder.ENCODED_LENGTH,
                header.blockLength(),
                header.version());

        long orderId = dec.orderId();
        long userId = dec.userId();
        String symbol = dec.symbol();
        String reason = readVar(b, dec.limit(), dec.reason());
        return new OrderRejectData(
                orderId,
                userId,
                symbol,
                reason,
                dec.remainingQty().mantissa(),
                dec.remainingQty().exponent(),
                dec.pricePresent() == 1,
                dec.price().mantissa(),
                dec.price().exponent(),
                dec.ts());
    }

    // ==================== 辅助 ====================

    private static void writeIntBE(byte[] buf, int pos, int v) {
        buf[pos] = (byte) (v >>> 24);
        buf[pos + 1] = (byte) (v >>> 16);
        buf[pos + 2] = (byte) (v >>> 8);
        buf[pos + 3] = (byte) v;
    }

    private static int readIntBE(byte[] buf, int pos) {
        return ((buf[pos] & 0xFF) << 24)
                | ((buf[pos + 1] & 0xFF) << 16)
                | ((buf[pos + 2] & 0xFF) << 8)
                | (buf[pos + 3] & 0xFF);
    }

    private static byte[] copyExact(byte[] buf, int len) {
        byte[] out = new byte[len];
        System.arraycopy(buf, 0, out, 0, len);
        return out;
    }

    private static int writeVar(MutableDirectBuffer b, int pos, String s, VarStringEncoder vs) {
        SymbolBytes cache = CTX.get().symbolCache;
        byte[] bytes;
        if (s != null && s.equals(cache.symbol)) {
            bytes = cache.bytes;
        } else {
            bytes = (s == null ? "" : s).getBytes(StandardCharsets.UTF_8);
            cache.symbol = s;
            cache.bytes = bytes;
        }
        b.putBytes(pos, bytes);
        vs.length(bytes.length);
        return bytes.length;
    }

    /** 最近一次 writeVar 的 symbol 字节缓存（单线程，撮合路径 symbol 恒定命中）。 */
    private static final class SymbolBytes {
        private String symbol;
        private byte[] bytes;
    }

    private static String readVar(DirectBuffer b, int pos, VarStringDecoder vs) {
        int n = (int) vs.length();
        if (n == 0) {
            return "";
        }
        byte[] bytes = new byte[n];
        b.getBytes(pos, bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static void writeDecimal(DecimalValueEncoder dv, java.math.BigDecimal v) {
        if (v == null) {
            dv.mantissa(0L).exponent((byte) 0);
            return;
        }
        BigInteger unscaled = v.unscaledValue();
        if (unscaled.bitLength() > 63) {
            throw new ArithmeticException("BigDecimal mantissa 溢出 int64: " + v);
        }
        int scale = v.scale();
        if (scale > 127 || scale < -128) {
            throw new ArithmeticException("BigDecimal scale 溢出 int8: " + v);
        }
        dv.mantissa(unscaled.longValue()).exponent((byte) -scale);
    }

    private static void writeDecimal(DecimalValueEncoder dv, Decimal v) {
        if (v == null) {
            dv.mantissa(0L).exponent((byte) 0);
            return;
        }
        dv.mantissa(v.mantissa()).exponent((byte) v.exponent());
    }

    private static void writeDecimal(DecimalValueEncoder dv, long mantissa, int exponent) {
        dv.mantissa(mantissa).exponent((byte) exponent);
    }

    private static Decimal readDecimal(DecimalValueDecoder dv) {
        return Decimal.of(dv.mantissa(), dv.exponent());
    }

    private static com.ocx.codec.sbe.OrderDirection toSbe(OrderDirection d) {
        return d == OrderDirection.SELL
                ? com.ocx.codec.sbe.OrderDirection.SELL
                : com.ocx.codec.sbe.OrderDirection.BUY;
    }

    private static OrderDirection fromSbe(com.ocx.codec.sbe.OrderDirection d) {
        return d == com.ocx.codec.sbe.OrderDirection.SELL
                ? OrderDirection.SELL
                : OrderDirection.BUY;
    }
}
