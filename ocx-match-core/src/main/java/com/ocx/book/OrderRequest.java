package com.ocx.book;

/**
 * 入簿/撮合请求 — 扁平字段集（替代 {@code Order} 对象）。
 *
 * <p>价格/数量为已定点 long（调用方按 symbol 精度归一化：价格 → coinScale 定点，
 * 数量 → baseCoinScale 定点），撮合仅做整数运算，零 Decimal/零对象分配。
 * 撮合线程复用的可变字段容器（Disruptor 单写者）。</p>
 */
public final class OrderRequest {

    /** 订单类型 code（对齐现有 Order 约定）。 */
    public static final int ORDER_TYPE_LIMIT = 0;

    public static final int ORDER_TYPE_MARKET = 1;

    /** TimeInForce code（GTC/IOC/FOK/POST_ONLY）。 */
    public static final int TIF_GTC = 0;

    public static final int TIF_IOC = 1;
    public static final int TIF_FOK = 2;
    public static final int TIF_POST_ONLY = 3;

    /** 方向：0=BUY 1=SELL（对齐 OrderDirection 数值约定）。 */
    public int direction;

    /** 订单 id（orderId→slot 映射键）。 */
    public long orderId;

    /** 用户 id（自成交保护/批撤按 userId 定位）。 */
    public long userId;

    /** 价格（coinScale 定点）。 */
    public long price;

    /** 原始数量（baseCoinScale 定点）。 */
    public long origQty;

    /** 冰山可见量（baseCoinScale 定点；0=普通单）。 */
    public long visibleQty;

    /** 订单类型（LIMIT/MARKET）。 */
    public int orderType = ORDER_TYPE_LIMIT;

    /** TimeInForce（GTC/IOC/FOK/POST_ONLY）。 */
    public int timeInForce = TIF_GTC;

    /** 仅减仓（合约）。 */
    public boolean reduceOnly;

    /** 入簿时间（ms）。 */
    public long time;

    /** 入簿前本单已累计成交额（coinScale 定点；taker 部分成交后剩余挂单用，
     *  保证 cumQuote 跨 taker/maker 阶段单调——2026-08-30 成交对账发现）。 */
    public long filledQuote;

    public void reset() {
        direction = 0;
        orderId = 0L;
        userId = 0L;
        price = 0L;
        origQty = 0L;
        visibleQty = 0L;
        orderType = ORDER_TYPE_LIMIT;
        timeInForce = TIF_GTC;
        reduceOnly = false;
        time = 0L;
        filledQuote = 0L;
    }
}
