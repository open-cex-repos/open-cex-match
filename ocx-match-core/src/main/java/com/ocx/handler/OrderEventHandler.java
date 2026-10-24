package com.ocx.handler;

import com.ocx.book.OrderBook;
import com.ocx.book.TriggerSlots;
import com.ocx.engine.MatchEngine;
import com.ocx.engine.MatchService;
import com.ocx.metrics.MatchMetrics;
import com.ocx.outbound.FillSink;
import com.ocx.outbound.MatchEventSink;
import com.ocx.outbound.OutboundDepth;
import com.ocx.snapshot.SnapshotCoordinator;

import org.agrona.concurrent.CachedEpochClock;

/**
 * 纵表链路事件处理器（最终形态）— 入站字段行（ingress 已解码，AeronOrderInSource
 * 纯偏移直读）→ {@link MatchEngine}，撮合/入簿/条件单/深度全走扁平字段，
 * 零 Order 对象。endOfBatch 统一发布深度 + 到点快照（输入 journal 恢复锚点）。</p>
 *
 * <p>撮合线程单写者（原 Disruptor 单消费者语义，2026-08 三进程化后由 AeronOrderInSource
 * 直调，不再依赖 LMAX Disruptor 库）。</p>
 */
public final class OrderEventHandler {

    private final OrderBook book;
    private final TriggerSlots triggers;
    private final MatchEngine engine;

    /** 撮合批时间戳缓存时钟（2026-08-30 E10：每批更新一次，替代逐点 System.currentTimeMillis）。 */
    private final CachedEpochClock clock = new CachedEpochClock();

    /** 本批最后入站帧 ts（2026-08-31：快照 freezeTs 改用它——与成交 recording
     * 的 fill.time 同钟（入站帧时间戳），锚点边界对账不再有 ms 级时钟错位）。 */
    private long lastInboundTs;

    private final FillSink outbound;
    private final MatchMetrics matchMetrics;
    private final MatchEventSink eventSink;
    private final String symbol;

    /** 批内成交暂存（引擎 fills 随即回收，先值拷贝入本批；批末统一打包出站）。 */
    private final com.ocx.book.ReusableFillList fillBatch = new com.ocx.book.ReusableFillList();

    /** 批内成交容器池（零分配循环复用）。 */
    private final com.ocx.book.ReusableFillList fillPool = new com.ocx.book.ReusableFillList();

    /** 批级时钟（LMAX：每批一次系统调用）。 */
    private long batchStartTimeMs;

    private long depthPublishIntervalMs = 500L;
    private long lastDepthPublishTs = 0L;

    /** 快照协调器（2026-08-29 输入 journal 恢复锚点；null = 不快照，压测/降级路径）。 */
    private SnapshotCoordinator snapshot;

    /** 恢复回放模式（输入 journal 重放期间置位）：成交不收集（重建期间不重复下发）。 */
    private boolean replayMode = false;

    public OrderEventHandler(
            OrderBook book,
            TriggerSlots triggers,
            MatchService matchService,
            FillSink outbound,
            MatchMetrics matchMetrics,
            MatchEventSink eventSink) {
        this.book = book;
        this.triggers = triggers;
        this.outbound = outbound;
        this.matchMetrics = matchMetrics;
        this.eventSink = eventSink;
        this.symbol = book.symbol();
        this.engine =
                new MatchEngine(
                        book,
                        triggers,
                        matchService,
                        fills -> {
                            if (replayMode) {
                                return;
                            }
                            for (int i = 0; i < fills.size(); i++) {
                                com.ocx.book.MatchFill src = fills.get(i);
                                com.ocx.book.MatchFill dst =
                                        fillPool.isEmpty()
                                                ? new com.ocx.book.MatchFill()
                                                : fillPool.remove(fillPool.size() - 1);
                                dst.direction = src.direction;
                                dst.symbol = src.symbol;
                                dst.takerOrderId = src.takerOrderId;
                                dst.makerOrderId = src.makerOrderId;
                                dst.price = src.price;
                                dst.priceExponent = src.priceExponent;
                                dst.qty = src.qty;
                                dst.qtyExponent = src.qtyExponent;
                                dst.takerCumQuote = src.takerCumQuote;
                                dst.makerCumQuote = src.makerCumQuote;
                                dst.turnOverExponent = src.turnOverExponent;
                                dst.time = src.time;
                                fillBatch.add(dst);
                            }
                        },
                        (orderId, userId, reason) -> {
                            if (matchMetrics != null) {
                                matchMetrics.recordRejectedOrder(symbol, reason);
                            }
                        },
                        matchMetrics);
        clock.update(System.currentTimeMillis());
        this.batchStartTimeMs = clock.time();
    }

    public void onBatchStart(long batchSize, long queueDepth) {
        if (matchMetrics != null) {
            matchMetrics.recordBatchSize(symbol, batchSize);
        }
        clock.update(System.currentTimeMillis());
        batchStartTimeMs = clock.time();
    }

    public void onEvent(OrderEvent event, long sequence, boolean endOfBatch) {
        if (event.isSnapshotRequest() || event.getSbeLength() == 0) {
            if (endOfBatch) {
                flushEndOfBatch();
            }
            return;
        }
        try {
            // 字段已由发布线程解码到槽位容器（消费线程解码，撮合线程零字节解析）
            // 撮合时间戳与入站 recording 同钟（帧时间戳）：成交 recording 的 fill.time
            // 与撤单/批撤时间可比，供成交对账 U5 校验（2026-08-30）。
            long ts = event.getTimestamp();
            if (ts != 0L) {
                lastInboundTs = ts;
            }
            engine.onOrderIn(event.getFields(), ts != 0L ? ts : batchStartTimeMs);
        } catch (Exception e) {
            emitEvent("handler_exception", e.getMessage());
        } finally {
            if (endOfBatch) {
                flushEndOfBatch();
            }
        }
    }

    /** 批末：深度发布 + 到点快照（输入 journal 恢复锚点）。 */
    void flushEndOfBatch() {
        flushFills();
        publishDepthSnapshot();
        engine.onBatchEnd();
        if (snapshot != null) {
            snapshot.onBatchEnd(lastInboundTs != 0L ? lastInboundTs : batchStartTimeMs);
        }
    }

    /** 入站源每 poll 批次结束回调：统一提交本批成交 + 深度 + 到点快照。 */
    public void onBatchEnd() {
        flushEndOfBatch();
    }

    /** 装配快照协调器（bootstrap 在入站订阅线程创建后调用；可空=不快照）。 */
    public void setSnapshotCoordinator(SnapshotCoordinator snapshot) {
        this.snapshot = snapshot;
    }

    /** 恢复回放模式开关（RecoveryService 回放期间置位，结束后复位）。 */
    public void setReplayMode(boolean replayMode) {
        this.replayMode = replayMode;
    }

    /** 撮合引擎（恢复回放入口，2026-08-29 输入 journal）。 */
    public MatchEngine getEngine() {
        return engine;
    }

    /** 订单簿（恢复回放/快照枚举）。 */
    public OrderBook getBook() {
        return book;
    }

    /** 条件单槽位（快照枚举/恢复挂载；null = 未装配条件单管理器）。 */
    public TriggerSlots getStops() {
        return triggers;
    }

    /** 批末统一提交本批成交（AeronFillSink 打包成单条消息 offer；随后容器归还池）。 */
    private void flushFills() {
        if (fillBatch.isEmpty()) {
            return;
        }
        if (outbound != null) {
            outbound.submitFills(fillBatch);
        }
        // 手工索引循环：避免 AbstractCollection.toArray/iterator 分配
        for (int i = 0; i < fillBatch.size(); i++) {
            fillPool.add(fillBatch.get(i));
        }
        fillBatch.clear();
    }

    /** 盘口深度快照（前 100 档，节流发布；撮合线程只拷贝，编码+发送在出站线程）。 */
    private void publishDepthSnapshot() {
        long now = clock.time();
        if (depthPublishIntervalMs > 0 && now - lastDepthPublishTs < depthPublishIntervalMs) {
            return;
        }
        lastDepthPublishTs = now;
        if (outbound == null) {
            return;
        }
        OutboundDepth depth = outbound.acquireDepth(symbol, book.coinScale(), book.baseCoinScale());
        try {
            depth.bidCount = book.copyLevels(0, depth.bidPrices, depth.bidVolumes, 100);
            depth.askCount = book.copyLevels(1, depth.askPrices, depth.askVolumes, 100);
            depth.ts = now;
            outbound.submitDepth(depth);
        } catch (Exception e) {
            emitEvent("depth_snapshot_error", e.getMessage());
        }
    }

    public OrderBook book() {
        return book;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setDepthPublishIntervalMs(long depthPublishIntervalMs) {
        this.depthPublishIntervalMs = depthPublishIntervalMs;
    }

    private void emitEvent(String event, String detail) {
        if (eventSink != null) {
            eventSink.emit(symbol, event, detail);
        }
    }
}
