package com.ocx.codec.domain;

/**
 * SBE OrderIn 帧直读字段容器（撮合线程热路径复用）。
 *
 * <p>全部原生字段、无 String/Decimal 中间态：字段由 {@code IoCodec.readOrderIn}
 * 按偏移直接读出（复用 CodecContext 内的 decoder，零分配），撮合线程填充 Order
 * 时不再经过字符串缓存/枚举 switch/时间解析。</p>
 */
public final class OrderInFields {

    /** 订单 id（0 非法）。 */
    public long id;

    /** 用户 id（Long.MIN_VALUE 非法；0 是合法值，market-maker 合成单）。 */
    public long userId;

    public long priceMantissa;
    public int priceExponent;

    public long origQtyMantissa;
    public int origQtyExponent;

    public long executedQtyMantissa;
    public int executedQtyExponent;

    /** 条件单触发价（mantissa=0 表示缺省）。 */
    public long mergeLowPriceMantissa;

    public int mergeLowPriceExponent;

    /** 0=BUY 1=SELL。 */
    public byte side;

    /** 1=LIMIT 2=MARKET 3=STOP_LIMIT 4=STOP_MARKET 5=TAKE_PROFIT_LIMIT 6=TAKE_PROFIT_MARKET。 */
    public byte type;

    /** 0=GTC 1=IOC 2=FOK 3=POST_ONLY（与 Order.TIF_* 一致）。 */
    public byte timeInForce;

    public byte status;

    /** epoch millis，0 表示缺省。 */
    public long createTimeMillis;

    /** 冰山可见量（mantissa=0 表示非冰山；0<visible<orig 为冰山单）。 */
    public long visibleQtyMantissa;

    public int visibleQtyExponent;

    /** 逐字段拷贝（发布线程解码 → 撮合线程槽位，纯原生赋值零分配）。 */
    public void copyFrom(OrderInFields src) {
        this.id = src.id;
        this.userId = src.userId;
        this.priceMantissa = src.priceMantissa;
        this.priceExponent = src.priceExponent;
        this.origQtyMantissa = src.origQtyMantissa;
        this.origQtyExponent = src.origQtyExponent;
        this.executedQtyMantissa = src.executedQtyMantissa;
        this.executedQtyExponent = src.executedQtyExponent;
        this.mergeLowPriceMantissa = src.mergeLowPriceMantissa;
        this.mergeLowPriceExponent = src.mergeLowPriceExponent;
        this.side = src.side;
        this.type = src.type;
        this.timeInForce = src.timeInForce;
        this.status = src.status;
        this.createTimeMillis = src.createTimeMillis;
        this.visibleQtyMantissa = src.visibleQtyMantissa;
        this.visibleQtyExponent = src.visibleQtyExponent;
    }
}
