package com.ocx.ingress;

import com.ocx.aeron.OcxAeron;
import com.ocx.outbound.DeadLetterSink;

import io.aeron.Publication;

import org.agrona.concurrent.UnsafeBuffer;

/**
 * 入站 DLT 死信发送器：SBE DLT 帧发布到 Archive 死信流（best-effort）。
 * 撮合链路已完全脱离 Kafka，死信同样只落 Archive 持久化日志。
 */
public final class AeronDeadLetterSink implements DeadLetterSink, AutoCloseable {

    private final Publication publication;
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[64 * 1024]);

    public AeronDeadLetterSink(OcxAeron aeron) {
        this.publication = aeron.addArchiveDltPublication();
    }

    @Override
    public void send(byte[] payload) {
        if (payload.length > buffer.capacity()) {
            throw new IllegalArgumentException("DLT payload too large: " + payload.length);
        }
        buffer.putBytes(0, payload);
        long result;
        int spins = 0;
        while ((result = publication.offer(buffer, 0, payload.length)) < 0) {
            if (++spins > 1_000_000) {
                throw new IllegalStateException("DLT archive backpressure: offer=" + result);
            }
            Thread.onSpinWait();
        }
    }

    @Override
    public void close() {
        publication.close();
    }
}
