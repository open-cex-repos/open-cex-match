package com.ocx.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ocx.recovery.RecoveryConfig;

import io.smallrye.config.ConfigValidationException;

import org.junit.jupiter.api.Test;

/**
 * SmallRye Config 装配测试：验证 yml/profile 加载、CLI/系统属性覆盖、
 * kebab-case 与 camelCase 宽松匹配、未知键快速失败（不再静默跳过）。
 */
class ConfigLoaderTest {

    @Test
    void loadsSpotProfileFromYamlWithDefaults() {
        MatchEngineConfig cfg = ConfigLoader.load(new String[] {"--spring.profiles.active=spot"});

        assertEquals("spot", cfg.getProfile());
        assertEquals("spot", cfg.getMatchEngineProperties().getEngine());
        assertEquals(6_000_000L, cfg.getMatchEngineProperties().getMaxOpenOrders());
        assertTrue(cfg.getMatchEngineProperties().getSymbols().isEmpty());
        assertEquals(4, cfg.getMatchEngineProperties().getDefaults().getCoinScale());
        assertTrue(cfg.isSelfTradePrevention());
        assertFalse(cfg.getRecoveryConfig().isEnabled());
        assertEquals("/metrics", cfg.getMetricsDataDir());
        assertEquals(200L, cfg.getDepthPublishIntervalMs());
    }

    @Test
    void loadsFuturesProfileWalDefaults() {
        MatchEngineConfig cfg =
                ConfigLoader.load(new String[] {"--spring.profiles.active=futures"});

        assertEquals("futures", cfg.getProfile());
        RecoveryConfig wal = cfg.getRecoveryConfig();
        assertTrue(wal.isEnabled());
        assertEquals("/data/match/wal", wal.getDataDir());
        assertEquals("aeron:udp?endpoint=localhost:8010", wal.getArchiveControlChannel());
        assertEquals("aeron:udp?endpoint=localhost:0", wal.getArchiveControlResponseChannel());
    }

    @Test
    void cliArgsOverrideYamlAndProfile() {
        MatchEngineConfig cfg =
                ConfigLoader.load(
                        new String[] {
                            "--spring.profiles.active=futures",
                            "--match.depth-publish-interval-ms=300",
                            "--spring.match.max-open-orders=123456",
                            "--match.wal.archive-control-channel=aeron:udp?endpoint=localhost:9010",
                        });

        assertEquals("futures", cfg.getProfile());
        assertEquals(300L, cfg.getDepthPublishIntervalMs());
        assertEquals(123456L, cfg.getMatchEngineProperties().getMaxOpenOrders());
        assertEquals(
                "aeron:udp?endpoint=localhost:9010",
                cfg.getRecoveryConfig().getArchiveControlChannel());
    }

    @Test
    void systemPropertiesBindKebabCaseMapNested() {
        System.setProperty("spring.match.symbols.BTCUSDT.coin-scale", "8");
        System.setProperty("spring.match.symbols.BTCUSDT.base-coin-scale", "2");
        System.setProperty("spring.match.symbols.BTCUSDT.tick-size", "0.00000001");
        System.setProperty("match.wal.data-dir", "/tmp/perf-big/wal");
        System.setProperty(
                "match.wal.archive-control-channel", "aeron:udp?endpoint=localhost:9020");
        try {
            MatchEngineConfig cfg =
                    ConfigLoader.load(new String[] {"--spring.profiles.active=spot"});

            MatchEngineProperties.CoinScale scale =
                    cfg.getMatchEngineProperties().getSymbols().get("BTCUSDT");
            assertNotNull(scale, "kebab 键 spring.match.symbols.BTCUSDT.coin-scale 应绑定到 CoinScale");
            assertEquals(8, scale.getCoinScale());
            assertEquals(2, scale.getBaseCoinScale());
            assertEquals("0.00000001", scale.getTickSize().orElse(null));
            assertEquals("/tmp/perf-big/wal", cfg.getRecoveryConfig().getDataDir());
            assertEquals(
                    "aeron:udp?endpoint=localhost:9020",
                    cfg.getRecoveryConfig().getArchiveControlChannel());
        } finally {
            System.clearProperty("spring.match.symbols.BTCUSDT.coin-scale");
            System.clearProperty("spring.match.symbols.BTCUSDT.base-coin-scale");
            System.clearProperty("spring.match.symbols.BTCUSDT.tick-size");
            System.clearProperty("match.wal.data-dir");
            System.clearProperty("match.wal.archive-control-channel");
        }
    }

    @Test
    void unknownKeyInMappingPrefixFailsFast() {
        System.setProperty("spring.match.no-such-key", "1");
        try {
            assertThrows(
                    ConfigValidationException.class,
                    () -> ConfigLoader.load(new String[] {"--spring.profiles.active=spot"}));
        } finally {
            System.clearProperty("spring.match.no-such-key");
        }
    }
}
