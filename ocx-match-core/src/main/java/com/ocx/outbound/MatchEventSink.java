package com.ocx.outbound;

/**
 * 撮合引擎关键事件通道（替代业务日志的可观测性出口）。
 *
 * <p>实现把 EVENT 行发布到 metrics 流（与 METRICS/OUTBOUND_STATS 同一通道），由独立
 * sidecar 订阅后暴露为 Prometheus 指标。事件低频（1 秒/事件名限流），用于呈现
 * WAL 失败、出站发送失败、Archive 背压、DLT 失败等"仅日志可见"的关键信号，
 * 撮合引擎自身不输出任何业务日志。</p>
 */
public interface MatchEventSink {

    /**
     * 实例级事件：写入全部 symbol 队列（如出站发布失败、发送线程异常）。
     * 队列尚未创建时（如启动早期）静默跳过。
     */
    void emitAll(String event, String detail);

    /** symbol 级事件：写入该 symbol 的 metrics 队列。 */
    void emit(String symbol, String event, String detail);
}
