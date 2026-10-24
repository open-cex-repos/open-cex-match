package com.ocx.snapshot;

/**
 * WAL ORDER_CREATE 扁平字段（纵表链路）— 替代恢复路径的 {@code Order} 对象。
 *
 * <p>price/origQty 为定点 long（encode 时 exponent 隐含 0）；恢复时按 symbol 的
 * coinScale/baseCoinScale 归一化（scaleMantissa 对 exponent=0 幂等，兼容旧文件
 * 的原始 exponent 字段）。</p>
 */
public final class SnapshotOrderFields {
    public long orderId;
    public long userId;
    public String symbol;

    /** coinScale 定点。 */
    public long price;

    /** price 原始 exponent（新字段版为 0，旧快照/旧 WAL 为实际值；恢复统一归一化）。 */
    public int priceExponent;

    /** baseCoinScale 定点。 */
    public long origQty;

    /** origQty 原始 exponent（同上）。 */
    public int origQtyExponent;

    /** 已成交量（baseCoinScale 定点；快照预填对账 U2 基线，2026-08-31）。 */
    public long executedQty;

    /** 累计成交额（coinScale 定点；快照预填对账 U1 基线，2026-08-31）。 */
    public long cumQuoteQty;

    /** 0=BUY 1=SELL。 */
    public int direction;

    /** GTC/IOC/FOK/POST_ONLY。 */
    public int tif;

    public long time;
}
