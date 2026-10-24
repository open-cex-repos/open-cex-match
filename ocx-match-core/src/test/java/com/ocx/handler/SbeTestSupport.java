package com.ocx.handler;

import com.ocx.codec.IoCodec;
import com.ocx.codec.domain.OrderIn;

/**
 * 测试辅助：把 {@link OrderIn} 编码为 SBE 帧并装入 {@link OrderEvent}，
 * 模拟消费线程"校验 + 路由 + memcpy"后的槽位形态，供 handler 直读。
 */
final class SbeTestSupport {

    private SbeTestSupport() {}

    /** 直接以 OrderIn 构造事件（覆盖 typeCode 3-6 / status 2 / status 99 等 Order 模型无法表达的入站形态）。 */
    static OrderEvent event(OrderIn orderIn) {
        return event(IoCodec.encodeOrderIn(orderIn), orderIn.symbol());
    }

    private static OrderEvent event(byte[] frame, String symbol) {
        OrderEvent event = new OrderEvent();
        System.arraycopy(frame, 0, event.getSbePayload(), 0, frame.length);
        event.setSbeLength(frame.length);
        event.setSymbol(symbol);
        event.setSnapshotRequest(false);
        event.setTimestamp(System.currentTimeMillis());
        // 模拟发布线程：解码字段到槽位容器（撮合线程直读）
        IoCodec.readOrderIn(IoCodec.context(), frame, frame.length, event.getFields());
        return event;
    }
}
