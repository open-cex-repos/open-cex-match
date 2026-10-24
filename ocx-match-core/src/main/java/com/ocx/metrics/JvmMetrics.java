package com.ocx.metrics;

/**
 * 撮合引擎进程级 JVM 指标快照。
 *
 * <p>字段全部为基础类型，内嵌于 METRICS 行经 metrics 流直达 sidecar。数值来源
 * {@link JvmMetricsSampler}；时间单位为毫秒（uptime 为毫秒）。</p>
 */
public record JvmMetrics(
        long heapUsed,
        long heapCommitted,
        long heapMax,
        long nonHeapUsed,
        long threadsLive,
        long threadsDaemon,
        long threadsPeak,
        long gcCollections,
        long gcCollectionTimeMillis,
        long gcLastPauseMillis,
        long uptimeMillis) {}
