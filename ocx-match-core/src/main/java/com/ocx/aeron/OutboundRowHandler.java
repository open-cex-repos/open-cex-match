package com.ocx.aeron;

import com.ocx.book.MatchFill;
import com.ocx.metrics.MetricsSnapshot;
import com.ocx.outbound.OutboundDepth;

/**
 * 出站行消费契约（core 定义，egress 实现）：撮合核心产出的成交/深度/事件/指标。
 * 状态变更 WAL 事件自 2026-08-28 走独立持久化流、2026-08-29 已整体退役，
 * 不再出现在出站成交环上。
 */
public interface OutboundRowHandler {

    void onFill(MatchFill fill);

    void onDepth(OutboundDepth depth);

    void onEvent(String symbol, String event, String detail);

    void onMetrics(MetricsSnapshot snapshot);

    /** 一个 poll 批次结束（成交批处理边界，默认无操作）。 */
    default void onBatchEnd() {}
}
