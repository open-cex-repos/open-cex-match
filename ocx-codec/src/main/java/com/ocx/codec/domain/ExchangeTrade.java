package com.ocx.codec.domain;

import com.ocx.codec.enums.OrderDirection;

/**
 * 成交记录（撮合协议出站对象）。
 *
 * <p>2026-08-25 数值字段 primitive 化：price/amount/buyTurnover/sellTurnover 拆为
 * {@code long mantissa + int exponent}（value = mantissa × 10^exponent），与 SBE
 * decimalValue wire 布局同构，processMatch 组装与 IoCodec 编解码零 Decimal 对象。
 * 消费侧如需 BigDecimal，用 {@code BigDecimal.valueOf(mantissa, -exponent)} 自行转换。</p>
 */
public class ExchangeTrade {

    private String symbol;
    private OrderDirection direction;
    private long priceMantissa;
    private int priceExponent;
    private long amountMantissa;
    private int amountExponent;

    private long buyOrderId;
    private long sellOrderId;
    private long buyTurnoverMantissa;
    private int buyTurnoverExponent;
    private long sellTurnoverMantissa;
    private int sellTurnoverExponent;

    private long time;

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public OrderDirection getDirection() {
        return direction;
    }

    public void setDirection(OrderDirection direction) {
        this.direction = direction;
    }

    public long getPriceMantissa() {
        return priceMantissa;
    }

    public int getPriceExponent() {
        return priceExponent;
    }

    public void setPrice(long mantissa, int exponent) {
        this.priceMantissa = mantissa;
        this.priceExponent = exponent;
    }

    public long getAmountMantissa() {
        return amountMantissa;
    }

    public int getAmountExponent() {
        return amountExponent;
    }

    public void setAmount(long mantissa, int exponent) {
        this.amountMantissa = mantissa;
        this.amountExponent = exponent;
    }

    public long getBuyOrderId() {
        return buyOrderId;
    }

    public void setBuyOrderId(long buyOrderId) {
        this.buyOrderId = buyOrderId;
    }

    public long getSellOrderId() {
        return sellOrderId;
    }

    public void setSellOrderId(long sellOrderId) {
        this.sellOrderId = sellOrderId;
    }

    public long getBuyTurnoverMantissa() {
        return buyTurnoverMantissa;
    }

    public int getBuyTurnoverExponent() {
        return buyTurnoverExponent;
    }

    public void setBuyTurnover(long mantissa, int exponent) {
        this.buyTurnoverMantissa = mantissa;
        this.buyTurnoverExponent = exponent;
    }

    public long getSellTurnoverMantissa() {
        return sellTurnoverMantissa;
    }

    public int getSellTurnoverExponent() {
        return sellTurnoverExponent;
    }

    public void setSellTurnover(long mantissa, int exponent) {
        this.sellTurnoverMantissa = mantissa;
        this.sellTurnoverExponent = exponent;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }
}
