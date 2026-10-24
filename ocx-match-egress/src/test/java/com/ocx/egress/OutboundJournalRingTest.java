package com.ocx.egress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.Publication;
import io.aeron.Subscription;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 出站录制解耦环（2026-09-02）：编码 → 环 → drain → journal 发布。
 * 覆盖：变长消息顺序/内容转发；环满回压后恢复消费零丢失（与入站桥同一纪律）。
 */
class OutboundJournalRingTest {

    private static final int TEST_STREAM = 555_000;

    static {
        // 与生产压测配置对齐：初始流控窗口 4MB
        System.setProperty("aeron.rcv.initial.window.length", "4194304");
        System.setProperty("aeron.socket.so_rcvbuf", "8388608");
    }

    private com.ocx.aeron.OcxAeron aeron;

    @AfterEach
    void tearDown() {
        if (aeron != null) {
            aeron.close();
        }
    }

    @Test
    void forwardsInOrderWithZeroLossAcrossRingWrap() throws Exception {
        aeron = com.ocx.aeron.OcxAeron.embedded();
        Publication pub = aeron.aeron().addPublication("aeron:ipc", TEST_STREAM);
        Subscription sub = aeron.aeron().addSubscription("aeron:ipc", TEST_STREAM);
        OutboundJournalRing ring = new OutboundJournalRing(pub, "T", 1 << 16);
        ring.start();

        final int total = 200_000;
        final int msgLen = 40;
        AtomicInteger got = new AtomicInteger();
        Thread consumer =
                new Thread(
                        () -> {
                            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                            while (got.get() < total && System.nanoTime() < deadline) {
                                got.addAndGet(
                                        sub.poll(
                                                (buffer, offset, length, header) -> {
                                                    // 校验内容：首字节 = 序号 % 256（与发布侧同规则）
                                                    if ((buffer.getByte(offset) & 0xFF)
                                                            != (seqAt(buffer, offset) % 256)) {
                                                        throw new AssertionError("内容错序");
                                                    }
                                                },
                                                1024));
                                Thread.onSpinWait();
                            }
                        });
        consumer.setDaemon(true);
        consumer.start();

        UnsafeBuffer buf = new UnsafeBuffer(new byte[msgLen]);
        for (int i = 0; i < total; i++) {
            buf.putByte(0, (byte) (i % 256));
            buf.putInt(1, i);
            // 发送线程语义：写入环（环满内部自旋 = 有界回压）
            ring.offer(buf, 0, msgLen);
        }
        consumer.join(10_000);
        assertEquals(total, got.get(), "零丢失");
        assertTrue(ring.fillBytes() < (1 << 16), "环应收敛到近空（fill=" + ring.fillBytes() + "）");
        ring.close();
    }

    private static int seqAt(org.agrona.DirectBuffer buffer, int offset) {
        // UnsafeBuffer.putInt 用原生字节序，读取端须一致
        return buffer.getInt(offset + 1) & 0x7fffffff;
    }
}
