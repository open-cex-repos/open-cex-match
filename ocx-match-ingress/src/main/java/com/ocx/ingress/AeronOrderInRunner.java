package com.ocx.ingress;

import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.IdleStrategy;

import java.util.Map;

/**
 * 入站 Aeron UDP 收单循环（替代已删除的 {@code KafkaConsumerRunner}）。
 *
 * <p>轮询各 symbol 的 UDP 入站订阅，把收到的 OCX2/SBE OrderIn 帧零拷贝交给
 * {@link MessageConsumerListener}（校验 + 路由 + 解码发布）。收单端到端不再经过 Kafka。</p>
 *
 * <p>背压链：core 慢 → 入站 IPC 环满 → {@code AeronOrderInSink.offerRetry} 阻塞本线程
 * → UDP 不再 poll → 媒体驱动接收缓冲满 → 生产端 offer 退避重试。</p>
 */
public final class AeronOrderInRunner implements AutoCloseable {

    private static final int FRAGMENT_LIMIT = 256;

    private final Map<String, Subscription> subscriptions;
    private final MessageConsumerListener listener;

    /** 收单时刻缓存时钟：每个非空 poll 批更新一次（替代逐帧 System.currentTimeMillis）。 */
    private final CachedEpochClock clock;

    /** 方法引用提升为字段：poll 每轮调用零分配（同 AeronOrderInSource 修复）。 */
    private final FragmentHandler fragmentHandler = this::onFragment;

    private final IdleStrategy idle = new BackoffIdleStrategy(100, 100, 1_000, 1_000_000);
    private final Thread thread;
    private volatile boolean running = true;

    public AeronOrderInRunner(
            Map<String, Subscription> subscriptions, MessageConsumerListener listener) {
        this(subscriptions, listener, new CachedEpochClock());
    }

    AeronOrderInRunner(
            Map<String, Subscription> subscriptions,
            MessageConsumerListener listener,
            CachedEpochClock clock) {
        this.subscriptions = subscriptions;
        this.listener = listener;
        this.clock = clock;
        clock.update(System.currentTimeMillis());
        this.thread = new Thread(this::run, "aeron-order-in");
        this.thread.setDaemon(true);
    }

    public void start() {
        thread.start();
    }

    private void run() {
        while (running) {
            int total = 0;
            for (Subscription sub : subscriptions.values()) {
                total += sub.poll(fragmentHandler, FRAGMENT_LIMIT);
            }
            if (total > 0) {
                // 批末更新缓存时钟：批内帧共享同一收单时刻（ms 精度，批间序正确）
                clock.update(System.currentTimeMillis());
            }
            if (total == 0) {
                idle.idle();
            }
        }
    }

    private void onFragment(
            DirectBuffer buffer, int offset, int length, io.aeron.logbuffer.Header header) {
        listener.onOrderIn(buffer, offset, length);
    }

    @Override
    public void close() {
        running = false;
    }
}
