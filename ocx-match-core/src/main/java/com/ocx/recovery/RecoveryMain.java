package com.ocx.recovery;

import com.ocx.book.OrderBook;
import com.ocx.book.TriggerSlots;
import com.ocx.config.ConfigLoader;
import com.ocx.config.MatchEngineConfig;
import com.ocx.config.MatchEngineProperties;
import com.ocx.engine.MatchService;
import com.ocx.handler.OrderEventHandler;

import java.util.HashMap;
import java.util.Map;

/**
 * WAL 恢复独立校验入口：加载引擎配置 → 按 symbol 建空订单簿 → 从 Aeron Archive
 * 回放 WAL recording，打印恢复统计。压测脚本在停链前用它验证崩溃恢复路径
 * （须在媒体驱动与 Archive 仍在运行时执行）。
 */
public final class RecoveryMain {

    private RecoveryMain() {}

    public static void main(String[] args) {
        MatchEngineConfig cfg = ConfigLoader.load(args);
        MatchEngineProperties props = cfg.getMatchEngineProperties();
        Map<String, OrderEventHandler> handlers = new HashMap<>();
        for (Map.Entry<String, MatchEngineProperties.CoinScale> entry :
                props.getSymbols().entrySet()) {
            String symbol = entry.getKey();
            MatchEngineProperties.CoinScale value = entry.getValue();
            int coinScale =
                    value != null ? value.getCoinScale() : props.getDefaults().getCoinScale();
            int baseCoinScale =
                    value != null
                            ? value.getBaseCoinScale()
                            : props.getDefaults().getBaseCoinScale();
            long cap = props.getMaxOpenOrders() > 0 ? props.getMaxOpenOrders() : 1_000_000L;
            int orderCapacity = (int) Math.min(cap, 1 << 30);
            int levelCapacity = (int) Math.min(Math.max(cap / 16, 1024), 1 << 20);
            OrderBook book =
                    new OrderBook(
                            symbol, coinScale, baseCoinScale, orderCapacity, levelCapacity, cap);
            TriggerSlots triggers =
                    new TriggerSlots((int) Math.min(Math.max(cap / 4, 1024), 1 << 20), 1 << 16);
            handlers.put(
                    symbol,
                    new OrderEventHandler(
                            book, triggers, new MatchService(false), null, null, null));
        }
        RecoveryService.WalRecoveryStats stats =
                new RecoveryService()
                        .recover(
                                cfg.getRecoveryConfig(),
                                System.getProperty("ocx.aeron.dir", "/tmp/ocx-aeron"),
                                handlers);
        System.err.println("[RecoveryMain] WAL_RECOVERY_DONE " + stats);
        for (Map.Entry<String, OrderEventHandler> e : handlers.entrySet()) {
            TriggerSlots triggers = e.getValue().getStops();
            System.err.println(
                    "[RecoveryMain] symbol="
                            + e.getKey()
                            + " openOrders="
                            + e.getValue().getBook().openOrderCount()
                            + " triggerOrders="
                            + (triggers == null ? 0 : triggers.orderCount()));
        }
    }
}
