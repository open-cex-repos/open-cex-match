package com.ocx.book;

/**
 * 成交记录（扁平字段，替代 {@code ExchangeTrade} 之前的内环载体）。
 * 撮合线程复用的可变容器；字段直接供出站 SBE 编码使用。
 */
public final class MatchFill {

    /** taker 方向（0=BUY 1=SELL）。 */
    public int direction;

    /** symbol（共享字符串引用，零拷贝）。 */
    public String symbol;

    /** taker 订单 id。 */
    public long takerOrderId;

    /** maker 订单 id。 */
    public long makerOrderId;

    /** 成交价（coinScale 定点）。 */
    public long price;

    /** 成交价原始 exponent（= -coinScale，出站 decimal 帧用）。 */
    public int priceExponent;

    /** 成交数量（baseCoinScale 定点）。 */
    public long qty;

    /** 成交数量原始 exponent（= -baseCoinScale，出站 decimal 帧用）。 */
    public int qtyExponent;

    /** taker 累计成交额（coinScale 定点）。 */
    public long takerCumQuote;

    /** maker 累计成交额（coinScale 定点）。 */
    public long makerCumQuote;

    /** 成交额原始 exponent（= -coinScale，出站 decimal 帧用）。 */
    public int turnOverExponent;

    /** 成交时间（ms）。 */
    public long time;

    public void reset() {
        direction = 0;
        symbol = null;
        takerOrderId = 0L;
        makerOrderId = 0L;
        price = 0L;
        priceExponent = 0;
        qty = 0L;
        qtyExponent = 0;
        takerCumQuote = 0L;
        makerCumQuote = 0L;
        turnOverExponent = 0;
        time = 0L;
    }
}
