package com.ocx.egress;

import io.aeron.Publication;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * 出站录制有界解耦环（2026-09-02，录制端解耦专项第二刀）：egress 发送线程把
 * 编码后的成交/深度消息写入本环（SPSC：发送线程写、drain 线程读），drain 线程
 * 按 Archive 录制端节奏发布到 journal 流——录制端卡顿由环吸收，撮合线程不再
 * 被 egress 出站队列回压。
 *
 * <p>行布局：{@code [4B len][msg bytes]}，行可跨环边界（写两段、读侧 scratch
 * 拷贝），环满时发送线程自旋（有界回压，绝不丢弃）。与入站桥同一套预算纪律：
 * 消费端 offer 失败自旋等待，不做任何丢帧路径。</p>
 */
public final class OutboundJournalRing implements Runnable {

    private static final int LEN_BYTES = 4;
    private static final long SPIN_PARK_NANOS = 1_000L;

    /** 缺省最大消息（深度帧量级）；成交批由调用方传入实际上限。 */
    private static final int DEFAULT_MAX_MSG = 8192;

    private final Publication journalPublication;
    private final String symbol;
    private final int maxMessage;
    private final byte[] ring;
    private final UnsafeBuffer ringBuffer;
    private final byte[] scratch;
    private final UnsafeBuffer scratchBuffer;
    private final IdleStrategy idle = new BackoffIdleStrategy(100, 100, 1_000, 1_000_000);

    private volatile long head; // 发送线程写
    private volatile long tail; // drain 线程读
    private Thread thread;
    private volatile boolean running = true;
    private boolean noSubscriberWarned;

    public OutboundJournalRing(Publication journalPublication, String symbol, int ringSizeBytes) {
        this(journalPublication, symbol, ringSizeBytes, DEFAULT_MAX_MSG);
    }

    public OutboundJournalRing(
            Publication journalPublication, String symbol, int ringSizeBytes, int maxMessage) {
        this.journalPublication = journalPublication;
        this.symbol = symbol;
        this.maxMessage = maxMessage;
        this.ring = new byte[ringSizeBytes];
        this.ringBuffer = new UnsafeBuffer(ring);
        this.scratch = new byte[maxMessage];
        this.scratchBuffer = new UnsafeBuffer(scratch);
    }

    public void start() {
        thread = new Thread(this, "outbound-journal-drain-" + symbol);
        thread.setDaemon(true);
        thread.start();
    }

    public void close() {
        running = false;
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** 环占用字节（监控）。 */
    public long fillBytes() {
        return head - tail;
    }

    /**
     * 发送线程：把一条消息写入环（[4B len][bytes]）。环满自旋（有界回压）。
     */
    public void offer(DirectBuffer buffer, int offset, int length) {
        if (length <= 0 || length > maxMessage) {
            throw new IllegalArgumentException("非法出站消息长度: " + length);
        }
        int need = LEN_BYTES + length;
        while (freeBytes() < need) {
            Thread.onSpinWait();
        }
        writeIntAt(head, length);
        int off = (int) ((head + LEN_BYTES) % ring.length);
        int first = Math.min(length, ring.length - off);
        ringBuffer.putBytes(off, buffer, offset, first);
        if (first < length) {
            ringBuffer.putBytes(0, buffer, offset + first, length - first);
        }
        head += need;
    }

    @Override
    public void run() {
        while (running) {
            if (drainOnce() == 0) {
                idle.idle();
            }
        }
        drainOnce();
    }

    /** drain 线程：从环读消息发布到 journal 流（录制端背压时自旋）。返回发布条数。 */
    int drainOnce() {
        int n = 0;
        while (tail < head) {
            int len = readIntAt(tail);
            if (len <= 0 || len > maxMessage) {
                // 防御：环内容损坏时停住而非空转（正常路径不可达）
                break;
            }
            int off = (int) ((tail + LEN_BYTES) % ring.length);
            long result;
            if (off + len <= ring.length) {
                result = journalPublication.offer(ringBuffer, off, len);
            } else {
                int first = ring.length - off;
                scratchBuffer.putBytes(0, ringBuffer, off, first);
                scratchBuffer.putBytes(first, ringBuffer, 0, len - first);
                result = journalPublication.offer(scratchBuffer, 0, len);
            }
            if (result < 0L) {
                if (!journalPublication.isConnected()) {
                    // 零落盘形态（record-outbound=false）：无订阅者，丢弃本帧继续
                    // （对齐旧 sender 的 no-subscriber 语义；一次性告警）
                    if (!noSubscriberWarned) {
                        noSubscriberWarned = true;
                        System.err.println(
                                "[OutboundJournalRing] " + symbol + " 无订阅者：出站帧丢弃（零落盘形态）");
                    }
                    tail += LEN_BYTES + len;
                    n++;
                    continue;
                }
                break; // 录制端背压：环吸收，下轮重试
            }
            tail += LEN_BYTES + len;
            n++;
        }
        return n;
    }

    private long freeBytes() {
        return ring.length - (head - tail);
    }

    private void writeIntAt(long ringOffset, int value) {
        int off = (int) (ringOffset % ring.length);
        for (int i = 0; i < 4; i++) {
            ring[(off + i) % ring.length] = (byte) (value >>> (24 - i * 8));
        }
    }

    private int readIntAt(long ringOffset) {
        int off = (int) (ringOffset % ring.length);
        int v = 0;
        for (int i = 0; i < 4; i++) {
            v = (v << 8) | (ring[(off + i) % ring.length] & 0xFF);
        }
        return v;
    }
}
