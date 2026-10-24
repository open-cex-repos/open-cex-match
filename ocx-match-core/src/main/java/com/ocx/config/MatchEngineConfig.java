package com.ocx.config;

import com.ocx.recovery.RecoveryConfig;

/**
 * 撮合引擎根配置聚合视图：持有 SmallRye Config 映射出的四个不可变映射
 * （{@code spring.match} / {@code match.wal} / {@code match}）
 * 与散点值 profile。撮合链路已完全脱离 Kafka，不再持有 bootstrap-servers。
 */
public final class MatchEngineConfig {

    private final MatchEngineProperties matchEngineProperties;
    private final RecoveryConfig walConfig;
    private final MatchTuningConfig matchTuningConfig;
    private final String profile;

    MatchEngineConfig(
            MatchEngineProperties matchEngineProperties,
            RecoveryConfig walConfig,
            MatchTuningConfig matchTuningConfig,
            String profile) {
        this.matchEngineProperties = matchEngineProperties;
        this.walConfig = walConfig;
        this.matchTuningConfig = matchTuningConfig;
        this.profile = profile;
    }

    public MatchEngineProperties getMatchEngineProperties() {
        return matchEngineProperties;
    }

    public RecoveryConfig getRecoveryConfig() {
        return walConfig;
    }

    public String getProfile() {
        return profile;
    }

    public long getDepthPublishIntervalMs() {
        return matchTuningConfig.getDepthPublishIntervalMs();
    }

    public boolean isSelfTradePrevention() {
        return matchTuningConfig.isSelfTradePrevention();
    }

    public String getMetricsDataDir() {
        return matchTuningConfig.getMetrics().getDataDir();
    }

    /** 指标发布开关（{@code -Dmatch.metrics.enabled=false} 禁用：egress 不发布出站统计/事件）。 */
    public boolean isMetricsEnabled() {
        return matchTuningConfig.getMetrics().isEnabled();
    }

    public long getMetricsLagTrigger() {
        return matchTuningConfig.getMetrics().getLagTrigger();
    }

    public long getMetricsLagRecover() {
        return matchTuningConfig.getMetrics().getLagRecover();
    }
}
