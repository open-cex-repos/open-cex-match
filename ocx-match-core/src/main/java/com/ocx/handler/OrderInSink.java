package com.ocx.handler;

import org.agrona.DirectBuffer;

/**
 * 入站订单槽位契约（由撮合核心定义，ingress 作为发布方实现）。
 *
 * <p>发布方（ingress）只做校验 + symbol 路由，把原始 SBE OrderIn 帧原样透传；
 * 撮合侧按偏移零拷贝直读字段（{@code IoCodec.readOrderIn}），不再有任何
 * 「解码 → 再编码 → 再解码」往返。timestamp 为 ingress 收单时刻，由槽位实现
 * 与帧一起写入 IPC 行头部。</p>
 */
public interface OrderInSink {

    /** 透传一笔原始 SBE 订单帧。frame 为 Aeron 片段（发布线程复用），实现必须同步拷贝。 */
    void publish(DirectBuffer frame, int offset, int length, long timestamp);

    /** 快照采集请求：撮合线程作为批末触发器处理，不参与撮合。 */
    void publishSnapshotRequest();
}
