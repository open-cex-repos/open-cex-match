package com.ocx.aeron;

import com.ocx.handler.OrderInSink;

import io.aeron.Publication;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * 入站槽位的 Aeron 实现（ingress 进程侧）：把原始 SBE 订单帧透传发布到 IPC 环。
 *
 * <p>IPC 行 = 8B 收单时刻 + 原始 SBE 帧（变长行，按实际帧长 offer，上限
 * {@link AeronWire#INBOUND_ROW_SIZE}）；offer 负返回时以退避自旋
 * 阻塞 ingress 消费线程，形成「Archive/egress 慢 → core 慢 → ingress 慢」的背压链。</p>
 */
public final class AeronOrderInSink implements OrderInSink {

    private final Publication publication;
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[AeronWire.INBOUND_ROW_SIZE]);
    private final IdleStrategy idle = new BackoffIdleStrategy(100, 100, 1_000, 1_000_000);

    public AeronOrderInSink(Publication publication) {
        this.publication = publication;
    }

    @Override
    public void publish(DirectBuffer frame, int offset, int length, long timestamp) {
        AeronWire.encodeInboundFrame(buffer, frame, offset, length, timestamp);
        offerRetry(AeronWire.INBOUND_TS_LENGTH + length);
    }

    @Override
    public void publishSnapshotRequest() {
        // 快照采集请求通道后续补齐（当前快照由 core 批末自行触发）。
    }

    private void offerRetry(int length) {
        long result;
        while ((result = publication.offer(buffer, 0, length)) < 0) {
            idle.idle();
        }
    }
}
