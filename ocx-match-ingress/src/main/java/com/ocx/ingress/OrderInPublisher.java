package com.ocx.ingress;

import com.ocx.codec.IoCodec;
import com.ocx.handler.OrderInSink;
import com.ocx.outbound.MatchEventSink;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.CachedEpochClock;

import java.util.List;

/**
 * 入站 SBE 透传发布器：把 Aeron 入站片段（OCX2/SBE OrderIn 帧）按 symbol 路由后
 * 经 {@link OrderInSink} 原样发布（帧 + 收单时刻），撮合核心按偏移零拷贝直读字段。
 *
 * <p>热路径：Aeron 片段校验后原样透传 → 按 symbol 索引直取槽位发布，一次 memcpy、
 * 零分配、零字段解析、零哈希查找（symbol 索引由路由层返回，避开 HashMap.get +
 * String.hashCode——5M JFR 实测该组合占收单线程 45% 采样）。</p>
 */
public class OrderInPublisher {

    /** 槽位数组：下标与路由层 SymbolRoute 索引一一对应（配置 symbol 顺序）。 */
    private final List<OrderInSink> sinks;

    private final MatchEventSink eventSink;

    /** 收单时刻缓存时钟（2026-08-30：AeronOrderInRunner 每 poll 批更新一次，
     *  替代逐帧 System.currentTimeMillis——5M TPS 下逐帧时钟是收单线程热点）。 */
    private final CachedEpochClock clock;

    public OrderInPublisher(
            List<OrderInSink> sinks, MatchEventSink eventSink, CachedEpochClock clock) {
        this.sinks = sinks;
        this.eventSink = eventSink;
        this.clock = clock;
    }

    /**
     * 发布 SBE OrderIn 帧（调用方已校验魔数/templateId/长度并按 symbol 路由）。
     * 原样透传 + 收单时刻。symbolIndex 由路由层给出（≥0，越界由调用方保证）。
     */
    public void onData(DirectBuffer payload, int offset, int length, int symbolIndex) {
        if (length > IoCodec.MAX_ORDER_IN_FRAME_LENGTH) {
            emitAll(
                    "frame_too_long",
                    "len=" + length + " cap=" + IoCodec.MAX_ORDER_IN_FRAME_LENGTH);
            return;
        }
        // 原始 SBE 帧透传：撮合线程按偏移直读（零解码往返）
        sinks.get(symbolIndex).publish(payload, offset, length, clock.time());
    }

    /** 投递快照采集请求到指定 symbol 索引的槽位（撮合线程批末 endOfBatch 触发快照）。 */
    public void requestSnapshot(int symbolIndex) {
        sinks.get(symbolIndex).publishSnapshotRequest();
    }

    private void emitAll(String event, String detail) {
        if (eventSink != null) {
            eventSink.emitAll(event, detail);
        }
    }
}
