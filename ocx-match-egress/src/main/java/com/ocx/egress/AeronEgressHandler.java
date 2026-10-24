package com.ocx.egress;

import com.ocx.aeron.OutboundRowHandler;
import com.ocx.book.MatchFill;
import com.ocx.metrics.MetricsSnapshot;
import com.ocx.outbound.FillSink;
import com.ocx.outbound.OutboundDepth;

import java.util.ArrayList;
import java.util.List;

/**
 * 出站行消费处理器（egress 进程侧）：成交批量入队 Aeron Archive 发布、深度提交。
 * 事件/指标自 2026-08-30 起不再经出站环到 egress——core 事件直发 metrics 流、
 * sidecar 直接订阅（Chronicle metrics 队列已删）。全部值容器由本类池化复用。
 */
public final class AeronEgressHandler implements OutboundRowHandler {

    private static final int BATCH_TARGET = 512;

    private final FillSink outbound;

    /** egress 线程独占：成交批容器池 + 本批列表。 */
    private final List<MatchFill> fillPool = new ArrayList<>(1024);

    private final List<MatchFill> batch = new ArrayList<>(BATCH_TARGET);

    public AeronEgressHandler(FillSink outbound) {
        this.outbound = outbound;
    }

    @Override
    public void onFill(MatchFill fill) {
        MatchFill copy = acquire();
        copy.direction = fill.direction;
        copy.symbol = fill.symbol;
        copy.takerOrderId = fill.takerOrderId;
        copy.makerOrderId = fill.makerOrderId;
        copy.price = fill.price;
        copy.priceExponent = fill.priceExponent;
        copy.qty = fill.qty;
        copy.qtyExponent = fill.qtyExponent;
        copy.takerCumQuote = fill.takerCumQuote;
        copy.makerCumQuote = fill.makerCumQuote;
        copy.turnOverExponent = fill.turnOverExponent;
        copy.time = fill.time;
        batch.add(copy);
        if (batch.size() >= BATCH_TARGET) {
            flushBatch();
        }
    }

    @Override
    public void onDepth(OutboundDepth depth) {
        outbound.submitDepth(depth);
    }

    @Override
    public void onEvent(String symbol, String event, String detail) {
        // 2026-08-30 起事件直发 metrics 流（core AeronEventSink），出站环不再承载 EVENT 行
    }

    @Override
    public void onMetrics(MetricsSnapshot snapshot) {
        // 2026-08-30 起 sidecar 直接订阅 metrics 流，egress 不再消费 METRICS 行
    }

    @Override
    public void onBatchEnd() {
        flushBatch();
    }

    private void flushBatch() {
        if (batch.isEmpty()) {
            return;
        }
        outbound.submitFills(batch);
        fillPool.addAll(batch);
        batch.clear();
    }

    private MatchFill acquire() {
        if (fillPool.isEmpty()) {
            return new MatchFill();
        }
        return fillPool.remove(fillPool.size() - 1);
    }
}
