package com.ocx.aeron;

import org.agrona.concurrent.status.CountersReader;

/**
 * journal 耐久位置读取（core 侧，2026-09-01 录制解耦）：从媒体驱动计数器
 * 读取 {@link InboundJournalBridge} 上报的 {@code (durableLivePos, durableJournalPos)}。
 * 计数器按 label {@code journal-durable-*} 匹配（跨进程，无类型依赖）。
 */
public final class JournalDurability {

    private JournalDurability() {}

    /** 返回 {livePos, journalPos}；计数器缺失（桥未就绪/兼容模式）时返回 null。 */
    public static long[] read(CountersReader counters, String symbol) {
        final long[] out = {-1L, -1L};
        counters.forEach(
                (counterId, typeId, labelBuffer, label) -> {
                    if (label.startsWith("journal-durable-live-pos:" + symbol)) {
                        out[0] = counters.getCounterValue(counterId);
                    } else if (label.startsWith("journal-durable-journal-pos:" + symbol)) {
                        out[1] = counters.getCounterValue(counterId);
                    }
                });
        return out[0] >= 0L && out[1] >= 0L ? out : null;
    }
}
