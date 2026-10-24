package com.ocx.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.ocx.aeron.StreamIds;

import org.junit.jupiter.api.Test;

/** 交叉校验：网关 stream id 计算与引擎 StreamIds.inbound 完全一致（防流错位）。 */
class InboundStreamIdsTest {

    @Test
    void matchesEngineStreamIds() {
        for (String symbol : new String[] {"BTCUSDT", "ETHUSDT", "SOLUSDT", "X"}) {
            assertEquals(
                    StreamIds.inbound(symbol),
                    InboundStreamIds.inbound(symbol),
                    "网关 inbound stream id 必须与引擎一致: " + symbol);
        }
    }
}
