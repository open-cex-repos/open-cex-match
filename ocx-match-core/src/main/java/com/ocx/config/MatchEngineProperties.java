package com.ocx.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

import java.util.Map;
import java.util.Optional;

/**
 * 撮合引擎业务配置（前缀 {@code spring.match}，键名保持与旧部署一致）。
 * 由 SmallRye Config 映射为不可变实现；{@code getSymbols()} 返回
 * {@code Map<String, CoinScale>}，map key 对应属性路径中的 symbol 段。
 */
@ConfigMapping(prefix = "spring.match")
public interface MatchEngineProperties {

    /** spot / futures，必填。 */
    @WithName("engine")
    String getEngine();

    /**
     * 全局挂单数上限（限价簿 + 条件单合计，内存硬闸；防 OOM）。
     * 默认 600 万（配合 4g 堆，高峰 500 万在簿 + 20% 余量）；≤0 表示不启用。
     */
    @WithName("max-open-orders")
    @WithDefault("6000000")
    long getMaxOpenOrders();

    @WithName("symbols")
    Map<String, CoinScale> getSymbols();

    @WithName("defaults")
    Defaults getDefaults();

    interface Defaults {
        @WithName("coin-scale")
        @WithDefault("4")
        int getCoinScale();

        @WithName("base-coin-scale")
        @WithDefault("4")
        int getBaseCoinScale();
    }

    interface CoinScale {
        @WithName("coin-scale")
        int getCoinScale();

        @WithName("base-coin-scale")
        int getBaseCoinScale();

        @WithName("tick-size")
        Optional<String> getTickSize();

        @WithName("step-size")
        Optional<String> getStepSize();
    }
}
