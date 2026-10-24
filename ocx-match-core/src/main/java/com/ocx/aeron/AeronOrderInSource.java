package com.ocx.aeron;

import com.ocx.codec.IoCodec;
import com.ocx.codec.IoCodec.CodecContext;
import com.ocx.handler.OrderEvent;
import com.ocx.handler.OrderEventHandler;

import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.YieldingIdleStrategy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 入站订阅循环（core 撮合线程）：poll IPC 环 → 解码进循环复用槽位 →
 * 直调 {@link OrderEventHandler}（零分配，等价原 Disruptor 消费者语义）。
 *
 * <p>idle 策略经 {@code -Docx.match.idle-strategy=busy-spin|yielding|backoff|sleeping} 配置，
 * 默认 {@code backoff}（激进参数 spins=10000）：共享核（macOS 4 P-core）下空转大部分时间
 * 自旋保持低唤醒延迟、偶发 yield/park 把核让给 ingress/egress/producer——700K 实测
 * BusySpin 在掉队空转时抢核导致 lag 2261→22 万（恶性循环），Backoff 避免该问题。
 * 独占核/Linux 绑核场景可显式传 {@code busy-spin}（LMAX 标准）。
 */
public final class AeronOrderInSource implements Runnable {

    private static final int FRAGMENT_LIMIT = 256;

    private final Subscription subscription;
    private final OrderEventHandler handler;
    private final String symbol;
    private final OrderEvent event = new OrderEvent();

    /**
     * 方法引用提升为字段：{@code poll(this::onFragment, ...)} 每轮调用都会创建方法引用
     * 对象——BusySpin 高频空转下 90s 分配 1.35TB（305 次 Young GC）。一次创建循环复用。
     */
    private final FragmentHandler fragmentHandler = this::onFragment;

    /** 撮合线程编解码上下文：启动时缓存一次，热路径免 ThreadLocal.get。 */
    private final CodecContext codecContext = IoCodec.context();

    private final IdleStrategy idle = createIdleStrategy();
    private volatile boolean running = true;

    /** 本 poll 批次已处理条数（首条触发 onBatchStart，等价 Disruptor 批时钟语义）。 */
    private int processed;

    public AeronOrderInSource(Subscription subscription, OrderEventHandler handler, String symbol) {
        this.subscription = subscription;
        this.handler = handler;
        this.symbol = symbol;
    }

    @Override
    public void run() {
        while (running) {
            processed = 0;
            int n = subscription.poll(fragmentHandler, FRAGMENT_LIMIT);
            if (n > 0) {
                // 每次 poll 结束显式回调批末（满负荷时 poll 恒返回 LIMIT，不能依赖
                // endOfBatch 标记——成交批次必须在此统一 flush）。
                handler.onBatchEnd();
            } else {
                idle.idle();
            }
        }
    }

    private void onFragment(
            DirectBuffer buffer, int offset, int length, io.aeron.logbuffer.Header header) {
        if (length < AeronWire.INBOUND_TS_LENGTH) {
            return;
        }
        if (processed == 0) {
            // 批时钟：每批一次系统调用，撮合线程直读（LMAX 惯例）。
            handler.onBatchStart(FRAGMENT_LIMIT, 0L);
        }
        processed++;
        long timestamp = AeronWire.readInboundTimestamp(buffer, offset);
        // 原始 SBE 帧直读（透传行：8B 时刻 + 帧区；变长行按实际长度读取）
        IoCodec.readOrderIn(
                codecContext,
                buffer,
                offset + AeronWire.INBOUND_TS_LENGTH,
                Math.min(length - AeronWire.INBOUND_TS_LENGTH, AeronWire.INBOUND_FRAME_LENGTH),
                event.getFields());
        event.setSymbol(symbol);
        event.setTimestamp(timestamp);
        event.setSbeLength(1);
        event.setSnapshotRequest(false);
        handler.onEvent(event, 0L, false);
    }

    /**
     * 入站 image 消费字节位置（快照锚点：与 Archive recording 同处流字节空间，
     * 直接与 recorder 位置取 min，免去行数→字节的帧开销换算）。无该 session 的 image 返回 -1。
     */
    public long imagePosition(int sessionId) {
        io.aeron.Image image = subscription.imageBySessionId(sessionId);
        return image == null ? -1L : image.position();
    }

    /**
     * 冻结时刻各入站 image 的消费位置快照（sessionId → 字节位置）：快照线程按
     * recording 的 sessionId 取锚点（与 {@link #imagePosition(int)} 同语义），
     * 但改在撮合线程上采集，避免快照线程并发读订阅 image 列表。
     */
    public Map<Integer, Long> imagePositions() {
        List<Image> images = subscription.images();
        if (images == null || images.isEmpty()) {
            return Map.of();
        }
        Map<Integer, Long> positions = new HashMap<>(images.size() * 2);
        for (Image image : images) {
            positions.put(image.sessionId(), image.position());
        }
        return positions;
    }

    public void stop() {
        running = false;
    }

    /** 撮合线程 idle 策略工厂：-Docx.match.idle-strategy，默认 backoff（激进参数）。 */
    static IdleStrategy createIdleStrategy() {
        String s = System.getProperty("ocx.match.idle-strategy", "backoff");
        return switch (s.trim().toLowerCase()) {
            case "busy-spin" -> new BusySpinIdleStrategy();
            case "yielding" -> new YieldingIdleStrategy();
            case "sleeping" -> new SleepingMillisIdleStrategy(1);
            default -> new BackoffIdleStrategy(10_000, 100, 1_000, 1_000_000);
        };
    }
}
