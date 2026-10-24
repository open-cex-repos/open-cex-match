package com.ocx.engine;

import com.ocx.book.MatchFill;
import com.ocx.book.OrderBook;
import com.ocx.book.OrderRequest;

import java.util.List;

/**
 * 字段版撮合服务 — 封装 {@link OrderBook} 的撮合主循环，补齐
 * FOK 预检 / POST_ONLY 拒绝 / 市价单语义 / 自成交保护。
 *
 * <p>撮合线程单写者，无状态（成交列表由调用方提供并负责 clear）。</p>
 */
public final class MatchService {

    /** 成交完成（剩余可能挂单）。 */
    public static final int OK = 0;

    /** FOK 流动性不足，整单拒绝（簿不变）。 */
    public static final int REJECT_FOK_INSUFFICIENT = 1;

    /** POST_ONLY 会立即成交，拒绝（簿不变）。 */
    public static final int REJECT_POST_ONLY_CROSS = 2;

    /** 重复 orderId / cap 满，拒绝。 */
    public static final int REJECT_ORDER_ACCEPT_FAILED = 3;

    /** 市价单簿空或不足，剩余部分丢弃（市价单不挂单）。 */
    public static final int OK_MARKET_PARTIAL = 4;

    /** 重复 orderId（撮合前预检，防止同号二次成交）。 */
    public static final int REJECT_DUPLICATE_ORDER_ID = 5;

    private final boolean selfTradePrevention;

    public MatchService(boolean selfTradePrevention) {
        this.selfTradePrevention = selfTradePrevention;
    }

    /**
     * 撮合一笔入站订单。
     *
     * @param req 撮合线程复用请求容器；撮合后 origQty 被改写为剩余量（GTC 挂单时即入簿量）
     * @param fills 撮合线程复用成交列表（调用方负责 clear；本方法追加）
     * @param wal WAL 钩子（null 禁用）
     */
    public int match(OrderBook book, OrderRequest req, long nowMs, List<MatchFill> fills) {
        // 判重提前到撮合前（orderId→slot 映射 O(1)）：同号订单已在簿则整单拒绝，
        // 避免重复单再次参与撮合产生同号二次成交（旧实现在挂单路径才判重）
        if (book.contains(req.orderId)) {
            return REJECT_DUPLICATE_ORDER_ID;
        }
        boolean market = req.orderType == OrderRequest.ORDER_TYPE_MARKET;
        long originalPrice = req.price;

        // 市价单：价格视为无穷（BUY 吃穿全部卖档 / SELL 吃穿全部买档），剩余不挂单
        if (market) {
            req.price = req.direction == 0 ? Long.MAX_VALUE : Long.MIN_VALUE;
            req.timeInForce = OrderRequest.TIF_IOC; // matchTaker 对非 GTC 不挂单
        }

        if (req.timeInForce == OrderRequest.TIF_FOK) {
            long avail = book.availableVolume(req.direction, req.price, req.origQty);
            if (avail < req.origQty) {
                req.price = originalPrice;
                return REJECT_FOK_INSUFFICIENT;
            }
        }

        if (req.timeInForce == OrderRequest.TIF_POST_ONLY) {
            long best = book.bestPrice(req.direction == 0 ? 1 : 0);
            if (best != -1L && (req.direction == 0 ? best <= req.price : best >= req.price)) {
                req.price = originalPrice;
                return REJECT_POST_ONLY_CROSS;
            }
            req.timeInForce = OrderRequest.TIF_GTC; // 不会成交 → 转 GTC 挂单
        }

        long remaining = book.matchTaker(req, nowMs, selfTradePrevention, fills);
        // GTC 剩余挂单（复用请求对象，改数量后直接入簿，零分配）；cap 满可上报
        if (remaining > 0L && req.timeInForce == OrderRequest.TIF_GTC) {
            req.origQty = remaining;
            if (req.visibleQty > 0L) {
                req.visibleQty = Math.min(req.visibleQty, remaining);
            }
            if (book.addOrder(req, nowMs) != 0) {
                req.price = originalPrice;
                return REJECT_ORDER_ACCEPT_FAILED;
            }
        }
        if (market) {
            req.price = originalPrice;
            return remaining > 0L ? OK_MARKET_PARTIAL : OK;
        }
        return OK;
    }
}
