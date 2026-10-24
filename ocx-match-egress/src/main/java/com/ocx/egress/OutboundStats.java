package com.ocx.egress;

/**
 * 出站发布侧统计（egress → Archive）：发布字节数与背压次数。
 * 由 {@link EgressMetricsPublisher} 实现，以 OUTBOUND_STATS 行发布到 metrics 流。
 */
public interface OutboundStats {

    /** 记录一次成功的 archive 发布（按 symbol 累积字节数）。 */
    void recordPublished(String symbol, long bytes);

    /** 记录一次 archive 背压（offer 返回负值）。 */
    void recordBackpressure(String symbol);
}
