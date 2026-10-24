package com.ocx.metrics;

/**
 * 撮合引擎指标快照（METRICS 行经 metrics 流直达 sidecar，供其聚合并暴露给 Prometheus）。
 *
 * <p>字段全部为扁平基础类型，便于跨进程二进制传输；时间戳单位统一为 epoch 毫秒。
 * 延迟分位数为 limit 类型撮合耗时（毫秒）。</p>
 */
public record MetricsSnapshot(
        String symbol,
        long exportTs,
        long lastOrderTs,
        boolean overloaded,
        long consumerLag,
        long inboundBacklog,
        double throughput,
        long ordersLimit,
        long ordersMarket,
        long tradesTotal,
        long rejectedTotal,
        long orderBookCapRejected,
        long openOrderCount,
        long orderBookCap,
        int depthBid,
        int depthAsk,
        double durationP50,
        double durationP95,
        double durationP99,
        double durationP999,
        double durationMax,
        long batchSizeMax,
        JvmMetrics jvm,
        long slotsUsed,
        long outboundBackpressure,
        long buyOrders,
        long sellOrders) {}
