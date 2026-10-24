package com.ocx.metrics;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class MatchMetricsTest {

    @Test
    void 订单与成交计数进入快照() {
        MatchMetrics m = new MatchMetrics();
        m.recordOrder("BTCUSDT", "limit");
        m.recordOrder("BTCUSDT", "limit");
        m.recordOrder("BTCUSDT", "market");
        m.recordTrades("BTCUSDT", 3);

        MetricsSnapshot s = m.snapshot("BTCUSDT", false);
        assertEquals(2L, s.ordersLimit());
        assertEquals(1L, s.ordersMarket());
        assertEquals(3L, s.tradesTotal());
    }

    @Test
    void 拒绝订单总数进入快照() {
        MatchMetrics m = new MatchMetrics();
        m.recordRejectedOrder("BTCUSDT", "price_invalid_for_limit");
        m.recordRejectedOrder("BTCUSDT", "side_invalid");
        assertEquals(2L, m.snapshot("BTCUSDT", false).rejectedTotal());
    }

    @Test
    void 消费积压与TPS被设置() {
        MatchMetrics m = new MatchMetrics();
        m.setConsumerLag(42);
        m.setCurrentTps(1234.5);
        assertEquals(42L, m.getConsumerLag());
        assertEquals(1234.5, m.snapshot("BTCUSDT", false).throughput(), 0.001);
    }

    @Test
    void recordMatch合并计数_成交与延迟一次记录() {
        MatchMetrics m = new MatchMetrics();
        m.recordMatch("BTCUSDT", "limit", 3, 5L);
        m.recordMatch("BTCUSDT", "limit", 0, 7L);
        m.recordMatch("BTCUSDT", "market", 2, 9L);

        MetricsSnapshot s = m.snapshot("BTCUSDT", false);
        assertEquals(2L, s.ordersLimit(), "限价单计数应合并累计");
        assertEquals(1L, s.ordersMarket(), "市价单计数应合并累计");
        assertEquals(5L, s.tradesTotal(), "成交计数应只累计正数");
        // 延迟直方图有记录：p50 应落在 [5,9] 区间（bucket 值域内）
        assertTrue(
                s.durationP50() >= 5.0 && s.durationP50() <= 9.0,
                "延迟直方图应记录到 recordMatch 的 elapsed，实际 p50=" + s.durationP50());
    }

    @Test
    void batch聚合与逐单计数对账() {
        // 同一输入序列：3 笔限价（1 成交）+ 2 笔市价（3 成交），batch 模式批末一次提交
        MatchMetrics full = new MatchMetrics(MatchMetrics.Mode.FULL);
        MatchMetrics batch = new MatchMetrics(MatchMetrics.Mode.BATCH);
        full.recordMatch("BTCUSDT", "limit", 1, 0L);
        full.recordMatch("BTCUSDT", "limit", 0, 0L);
        full.recordMatch("BTCUSDT", "limit", 0, 0L);
        full.recordMatch("BTCUSDT", "market", 3, 0L);
        full.recordMatch("BTCUSDT", "market", 0, 0L);

        batch.recordBatchMatch("BTCUSDT", 3, 2, 4, 1_000_000L);

        MetricsSnapshot fs = full.snapshot("BTCUSDT", false);
        MetricsSnapshot bs = batch.snapshot("BTCUSDT", false);
        assertEquals(fs.ordersLimit(), bs.ordersLimit(), "限价单计数应与逐单一致");
        assertEquals(fs.ordersMarket(), bs.ordersMarket(), "市价单计数应与逐单一致");
        assertEquals(fs.tradesTotal(), bs.tradesTotal(), "成交计数应与逐单一致");
    }

    @Test
    void off模式全部noop() {
        MatchMetrics m = new MatchMetrics(MatchMetrics.Mode.OFF);
        m.recordMatch("BTCUSDT", "limit", 3, 5L);
        m.recordBatchMatch("BTCUSDT", 1, 1, 2, 10L);
        m.recordRejectedOrder("BTCUSDT", "price_invalid_for_limit");
        m.recordOrderBookCapReject();

        MetricsSnapshot s = m.snapshot("BTCUSDT", false);
        assertEquals(0L, s.ordersLimit(), "OFF 模式不记录限价单");
        assertEquals(0L, s.ordersMarket(), "OFF 模式不记录市价单");
        assertEquals(0L, s.tradesTotal(), "OFF 模式不记录成交");
        assertEquals(0L, s.rejectedTotal(), "OFF 模式不记录拒绝");
    }

    @Test
    void 模式属性解析默认batch() {
        String old = System.getProperty("ocx.match.metrics.mode");
        try {
            System.clearProperty("ocx.match.metrics.mode");
            assertEquals(MatchMetrics.Mode.BATCH, MatchMetrics.modeFromProperty(), "未配置应默认 BATCH");
            System.setProperty("ocx.match.metrics.mode", "full");
            assertEquals(MatchMetrics.Mode.FULL, MatchMetrics.modeFromProperty());
            System.setProperty("ocx.match.metrics.mode", "off");
            assertEquals(MatchMetrics.Mode.OFF, MatchMetrics.modeFromProperty());
            System.setProperty("ocx.match.metrics.mode", "garbage");
            assertEquals(MatchMetrics.Mode.BATCH, MatchMetrics.modeFromProperty(), "非法值应回退 BATCH");
        } finally {
            if (old == null) {
                System.clearProperty("ocx.match.metrics.mode");
            } else {
                System.setProperty("ocx.match.metrics.mode", old);
            }
        }
    }
}
