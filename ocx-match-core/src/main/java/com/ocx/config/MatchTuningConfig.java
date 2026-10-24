package com.ocx.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

/**
 * 撮合引擎散点配置（前缀 {@code match}）：metrics、撮合节拍等。
 * 由 SmallRye Config 映射为不可变实现。
 * 2026-08-28：入站改为 Aeron UDP 收单，Kafka 消费配置（topic-pattern/consumer-group/
 * fetch-max-records）已移除。2026-08-30：快照开关（实际用 -Docx.snapshot.interval-ms）
 * 与 reconcile 残留配置删除。
 */
@ConfigMapping(prefix = "match")
public interface MatchTuningConfig {

    @WithName("self-trade-prevention")
    @WithDefault("true")
    boolean isSelfTradePrevention();

    @WithName("metrics")
    Metrics getMetrics();

    @WithName("depth-publish-interval-ms")
    @WithDefault("200")
    long getDepthPublishIntervalMs();

    interface Metrics {
        @WithName("enabled")
        @WithDefault("true")
        boolean isEnabled();

        @WithName("data-dir")
        @WithDefault("/metrics")
        String getDataDir();

        @WithName("ringbuffer-trigger")
        @WithDefault("1000")
        long getRingbufferTrigger();

        @WithName("ringbuffer-recover")
        @WithDefault("2000")
        long getRingbufferRecover();

        @WithName("lag-trigger")
        @WithDefault("5000")
        long getLagTrigger();

        @WithName("lag-recover")
        @WithDefault("2000")
        long getLagRecover();
    }
}
