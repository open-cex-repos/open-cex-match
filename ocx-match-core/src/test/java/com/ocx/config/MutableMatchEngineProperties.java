package com.ocx.config;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * 测试用可变 {@link MatchEngineProperties} 实现：生产装配由 SmallRye Config
 * 生成不可变实现，单测里需要手工构造时用本类（保持原 POJO 的 setter 风格）。
 */
public final class MutableMatchEngineProperties implements MatchEngineProperties {

    private String engine;
    private long maxOpenOrders = 6_000_000L;
    private Map<String, MatchEngineProperties.CoinScale> symbols = new LinkedHashMap<>();
    private Defaults defaults = new Defaults();

    @Override
    public String getEngine() {
        return engine;
    }

    @Override
    public long getMaxOpenOrders() {
        return maxOpenOrders;
    }

    @Override
    public Map<String, MatchEngineProperties.CoinScale> getSymbols() {
        return symbols;
    }

    @Override
    public Defaults getDefaults() {
        return defaults;
    }

    public MutableMatchEngineProperties setEngine(String engine) {
        this.engine = engine;
        return this;
    }

    public MutableMatchEngineProperties setMaxOpenOrders(long maxOpenOrders) {
        this.maxOpenOrders = maxOpenOrders;
        return this;
    }

    public MutableMatchEngineProperties setSymbols(
            Map<String, MatchEngineProperties.CoinScale> symbols) {
        this.symbols = symbols;
        return this;
    }

    public MutableMatchEngineProperties setDefaults(Defaults defaults) {
        this.defaults = defaults;
        return this;
    }

    public static final class Defaults implements MatchEngineProperties.Defaults {
        private int coinScale = 4;
        private int baseCoinScale = 4;

        @Override
        public int getCoinScale() {
            return coinScale;
        }

        @Override
        public int getBaseCoinScale() {
            return baseCoinScale;
        }

        public Defaults setCoinScale(int coinScale) {
            this.coinScale = coinScale;
            return this;
        }

        public Defaults setBaseCoinScale(int baseCoinScale) {
            this.baseCoinScale = baseCoinScale;
            return this;
        }
    }

    public static final class CoinScale implements MatchEngineProperties.CoinScale {
        private int coinScale;
        private int baseCoinScale;
        private String tickSize;
        private String stepSize;

        @Override
        public int getCoinScale() {
            return coinScale;
        }

        @Override
        public int getBaseCoinScale() {
            return baseCoinScale;
        }

        @Override
        public Optional<String> getTickSize() {
            return Optional.ofNullable(tickSize);
        }

        @Override
        public Optional<String> getStepSize() {
            return Optional.ofNullable(stepSize);
        }

        public CoinScale setCoinScale(int coinScale) {
            this.coinScale = coinScale;
            return this;
        }

        public CoinScale setBaseCoinScale(int baseCoinScale) {
            this.baseCoinScale = baseCoinScale;
            return this;
        }

        public CoinScale setTickSize(String tickSize) {
            this.tickSize = tickSize;
            return this;
        }

        public CoinScale setStepSize(String stepSize) {
            this.stepSize = stepSize;
            return this;
        }
    }
}
