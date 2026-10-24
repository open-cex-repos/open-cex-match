package com.ocx.engine;

import com.ocx.book.MatchFill;
import com.ocx.book.OrderBook;
import com.ocx.book.OrderRequest;
import com.ocx.book.ReusableFillList;
import com.ocx.book.TriggerSlots;
import com.ocx.codec.domain.OrderInFields;
import com.ocx.math.Decimal;
import com.ocx.metrics.MatchMetrics;

import java.util.List;

/**
 * 纵表撮合引擎装配（新链路）— 直接消费 SBE 直读的 {@link OrderInFields}，
 * 零 Order 对象：校验 → 转换（scale 归一化）→ 批撤路由 → 撮合 → 成交/拒绝输出回调。
 *
 * <p>撮合线程单写者；条件单（type 3-6）由上层路由到条件单管理器（Phase 3）。</p>
 */
public final class MatchEngine {

    /** 成交输出（撮合线程回调；列表为本引擎复用缓冲，调用方需在回调内消费）。 */
    public interface FillCallback {
        void onFills(List<MatchFill> fills);
    }

    /** 拒绝输出（值语义）。 */
    public interface RejectSink {
        void onReject(long orderId, long userId, String reason);
    }

    private final OrderBook book;
    private final TriggerSlots triggers;
    private final MatchService matchService;
    private final FillCallback fillSink;
    private final RejectSink rejectSink;
    private final MatchMetrics matchMetrics;

    /** FULL 模式：逐单 nanoTime + recordMatch（原语义）。 */
    private final boolean fullMetrics;

    /** BATCH 模式：批级累加、批末一次提交（-Docx.match.metrics.mode=batch）。 */
    private final boolean batchMetrics;

    /** 撮合线程复用缓冲（Disruptor 单写者，零分配）。 */
    private final OrderRequest scratch = new OrderRequest();

    /** 本调用成交列表（逻辑清空，避免 ArrayList.clear 全量置空）。 */
    private final List<MatchFill> fills = new ReusableFillList();

    private final int[] triggerBuf;

    /** 最近成交价（coinScale 定点；触发检查优先用真实成交价）。 */
    private long lastFillPrice = -1L;

    /** BATCH 模式批级累加（撮合线程单写；onBatchEnd 提交后清零）。 */
    private long batchStartNanos;

    private long batchLimitCount;
    private long batchMarketCount;
    private long batchTrades;
    private boolean batchActive;

    public MatchEngine(
            OrderBook book,
            TriggerSlots triggers,
            MatchService matchService,
            FillCallback fillSink,
            RejectSink rejectSink,
            MatchMetrics matchMetrics) {
        this.book = book;
        this.triggers = triggers;
        this.matchService = matchService;
        this.fillSink = fillSink;
        this.rejectSink = rejectSink;
        this.matchMetrics = matchMetrics;
        this.fullMetrics = matchMetrics != null && matchMetrics.mode() == MatchMetrics.Mode.FULL;
        this.batchMetrics = matchMetrics != null && matchMetrics.mode() == MatchMetrics.Mode.BATCH;
        this.triggerBuf = new int[triggers == null ? 0 : triggers.capacity()];
    }

    public OrderBook book() {
        return book;
    }

    /**
     * 处理一笔入站订单（撮合线程调用）。
     *
     * @return 0=已处理（可能成交/挂单）；1=校验拒绝；2=批撤完成；3=条件单（未处理，Phase 3）；
     *         4=撤单完成；5=撤单未命中
     */
    public int onOrderIn(OrderInFields in, long nowMs) {
        String reject = validate(in);
        if (reject != null) {
            rejectSink.onReject(in.id, in.userId, reject);
            return 1;
        }

        // 批量撤单：status=99 仅携带 userId
        if (in.status == 99) {
            book.batchCancel(in.userId);
            return 2;
        }

        // 撤单请求：status=2 携带 orderId
        if (in.status == 2) {
            boolean cancelled = book.cancel(in.id);
            return cancelled ? 4 : 5;
        }

        // 条件单：挂载到 TriggerSlots（触发价/限价/数量按 symbol 精度归一化）
        if (in.type >= 3 && in.type <= 6) {
            if (triggers == null) {
                return 3; // 未装配条件单管理器
            }
            int triggerType = in.type - 3; // 3→STOP_LIMIT(0) ... 6→TAKE_PROFIT_MARKET(3)
            long trigger =
                    Decimal.scaleMantissa(
                            in.mergeLowPriceMantissa, in.mergeLowPriceExponent, book.coinScale());
            if (trigger <= 0L) {
                rejectSink.onReject(in.id, in.userId, "trigger_price_invalid");
                return 1;
            }
            long price =
                    in.type == 3 || in.type == 5
                            ? Decimal.scaleMantissa(
                                    in.priceMantissa, in.priceExponent, book.coinScale())
                            : 0L;
            int targetExp = Math.min(in.origQtyExponent, in.executedQtyExponent);
            long qty =
                    Decimal.scaleMantissa(
                            Decimal.subtractAligned(
                                    in.origQtyMantissa,
                                    in.origQtyExponent,
                                    in.executedQtyMantissa,
                                    in.executedQtyExponent,
                                    targetExp),
                            targetExp,
                            book.baseCoinScale());
            long time = in.createTimeMillis != 0L ? in.createTimeMillis : nowMs;
            int rc =
                    triggers.add(
                            in.id, in.userId, trigger, price, qty, in.side, triggerType, 0L, time);
            if (rc != 0) {
                rejectSink.onReject(
                        in.id, in.userId, rc == 2 ? "duplicate_order_id" : "trigger_book_cap");
                return 1;
            }
            return 6;
        }

        fillRequest(in, scratch, nowMs);
        fills.clear();
        long startTick = fullMetrics ? System.nanoTime() : 0L;
        int rc = matchService.match(book, scratch, nowMs, fills);
        long elapsedMs = fullMetrics ? (System.nanoTime() - startTick) / 1_000_000L : -1L;
        boolean hasFills = !fills.isEmpty();
        int fillCount = hasFills ? fills.size() : 0;
        // 成交先发：即使随后挂单失败（满簿 cap），已发生的成交必须出站（否则成交丢失）
        if (hasFills) {
            lastFillPrice = fills.get(fills.size() - 1).price;
            fillSink.onFills(fills);
            book.recycleFills(fills);
        }
        // 延迟/订单指标：对齐对象版语义——每次撮合调用都记录（FOK/POST_ONLY 预检失败除外）
        if (matchMetrics != null
                && rc != MatchService.REJECT_FOK_INSUFFICIENT
                && rc != MatchService.REJECT_POST_ONLY_CROSS) {
            if (batchMetrics) {
                if (!batchActive) {
                    batchActive = true;
                    batchStartNanos = System.nanoTime();
                }
                if (scratch.orderType == OrderRequest.ORDER_TYPE_MARKET) {
                    batchMarketCount++;
                } else {
                    batchLimitCount++;
                }
                batchTrades += fillCount;
            } else if (fullMetrics) {
                matchMetrics.recordMatch(
                        book.symbol(),
                        scratch.orderType == OrderRequest.ORDER_TYPE_MARKET ? "market" : "limit",
                        fillCount,
                        elapsedMs);
            }
        }
        if (rc == MatchService.REJECT_FOK_INSUFFICIENT) {
            rejectSink.onReject(in.id, in.userId, "fok_insufficient_liquidity");
            return 1;
        }
        if (rc == MatchService.REJECT_POST_ONLY_CROSS) {
            rejectSink.onReject(in.id, in.userId, "post_only_would_cross");
            return 1;
        }
        if (rc == MatchService.REJECT_DUPLICATE_ORDER_ID) {
            rejectSink.onReject(in.id, in.userId, "duplicate_order_id");
            return 1;
        }
        if (rc == MatchService.REJECT_ORDER_ACCEPT_FAILED) {
            if (matchMetrics != null) {
                matchMetrics.recordOrderBookCapReject();
            }
            rejectSink.onReject(in.id, in.userId, "order_book_cap");
            return 1;
        }
        // 撮合后触发检查（最新价 = 买一/卖一中间价），触发转限价/市价单
        processTriggers(nowMs);
        return 0;
    }

    /** BATCH 模式批末提交（由入站源 poll 批次结束回调；FULL/OFF 下为空操作）。 */
    public void onBatchEnd() {
        if (!batchActive) {
            return;
        }
        long elapsedNanos = System.nanoTime() - batchStartNanos;
        matchMetrics.recordBatchMatch(
                book.symbol(), batchLimitCount, batchMarketCount, batchTrades, elapsedNanos);
        batchActive = false;
        batchLimitCount = 0L;
        batchMarketCount = 0L;
        batchTrades = 0L;
    }

    /** 触发检查 + 触发转单（条件单离簿 → 按类型转市价/限价入簿撮合）。 */
    private void processTriggers(long nowMs) {
        if (triggers == null || triggers.isEmpty()) {
            return;
        }
        long lastPriceKey = lastFillPrice > 0L ? lastFillPrice : bookMidPrice();
        if (lastPriceKey <= 0L) {
            return;
        }
        int n = triggers.checkAndTrigger(lastPriceKey, triggerBuf);
        for (int i = 0; i < n; i++) {
            int slot = triggerBuf[i];
            scratch.reset();
            scratch.orderId = triggers.orderIds[slot];
            scratch.userId = triggers.userIds[slot];
            scratch.direction = triggers.isBuy(slot) ? 0 : 1;
            scratch.price = triggers.prices[slot];
            scratch.origQty = triggers.origQtys[slot];
            int type = triggers.triggerType(slot);
            scratch.orderType =
                    (type == TriggerSlots.TYPE_STOP_MARKET
                                    || type == TriggerSlots.TYPE_TAKE_PROFIT_MARKET)
                            ? OrderRequest.ORDER_TYPE_MARKET
                            : OrderRequest.ORDER_TYPE_LIMIT;
            scratch.timeInForce = OrderRequest.TIF_GTC;
            scratch.time = nowMs;

            fills.clear();
            matchService.match(book, scratch, nowMs, fills);
            if (!fills.isEmpty()) {
                fillSink.onFills(fills);
                book.recycleFills(fills);
            }
            triggers.release(slot);
        }
    }

    /** 盘口中间价回退：双方都有用中间价，单边用可用边。 */
    private long bookMidPrice() {
        long bid = book.bestPrice(0);
        long ask = book.bestPrice(1);
        if (bid > 0L && ask > 0L) {
            return (bid + ask) / 2L;
        }
        return bid > 0L ? bid : ask;
    }

    /** 入站字段校验（对齐旧链路 validateOrder 核心语义）。 */
    private String validate(OrderInFields f) {
        // 按类型校验（2026-08-29 与 IoCodec 按类型解码对齐）：撤单/批撤帧只携带
        // 该操作所需字段（解码层已把订单字段裁剪为零），不得按普通单语义校验
        if (f.status == 99) {
            return f.userId == Long.MIN_VALUE ? "user_id_null" : null;
        }
        if (f.status == 2) {
            return f.id == 0L ? "order_id_null" : null;
        }
        if (f.id == 0L) {
            return "order_id_null";
        }
        if (f.userId == Long.MIN_VALUE) {
            return "user_id_null";
        }
        if (f.origQtyMantissa <= 0) {
            return "orig_qty_invalid";
        }
        if (f.side != 0 && f.side != 1) {
            return "side_invalid";
        }
        if (f.type < 1 || f.type > 6) {
            return "order_type_invalid";
        }
        if ((f.type == 1 || f.type == 3 || f.type == 5) && f.priceMantissa <= 0) {
            return "price_invalid_for_limit";
        }
        if (f.timeInForce < 0 || f.timeInForce > 3) {
            return "time_in_force_invalid";
        }
        return null;
    }

    /** SBE 直读字段 → 定点请求（价格 coinScale / 数量 baseCoinScale，scale 归一化）。 */
    private void fillRequest(OrderInFields f, OrderRequest r, long nowMs) {
        r.reset();
        r.orderId = f.id;
        r.userId = f.userId;
        r.direction = f.side;
        r.price = Decimal.scaleMantissa(f.priceMantissa, f.priceExponent, book.coinScale());
        int targetExp = Math.min(f.origQtyExponent, f.executedQtyExponent);
        r.origQty =
                Decimal.scaleMantissa(
                        Decimal.subtractAligned(
                                f.origQtyMantissa,
                                f.origQtyExponent,
                                f.executedQtyMantissa,
                                f.executedQtyExponent,
                                targetExp),
                        targetExp,
                        book.baseCoinScale());
        r.visibleQty =
                f.visibleQtyMantissa > 0
                        ? Decimal.scaleMantissa(
                                f.visibleQtyMantissa, f.visibleQtyExponent, book.baseCoinScale())
                        : 0L;
        r.orderType = f.type == 2 ? OrderRequest.ORDER_TYPE_MARKET : OrderRequest.ORDER_TYPE_LIMIT;
        r.timeInForce = f.timeInForce;
        r.time = f.createTimeMillis != 0L ? f.createTimeMillis : nowMs;
    }
}
