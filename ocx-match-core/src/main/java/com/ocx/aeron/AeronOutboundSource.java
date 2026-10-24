package com.ocx.aeron;

import com.ocx.book.MatchFill;
import com.ocx.metrics.MetricsSnapshot;
import com.ocx.outbound.OutboundDepth;

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

/**
 * 出站订阅循环（egress 进程侧）：poll IPC 环 → 按类型解码 → 交给 {@link OutboundRowHandler}
 * （Aeron Archive 发布 / metrics 队列）。成交/深度值容器循环复用，handler 必须同步消费。
 * WAL 事件自 2026-08-28 起走独立持久化流（Archive 记录），不再出现在出站环上。
 */
public final class AeronOutboundSource implements Runnable {

    private static final int FRAGMENT_LIMIT = 256;

    private final Subscription subscription;

    /** 预绑定 symbol（egress 按流建订阅时已知）：解码跳过 16 字节 ASCII 读取，零字符串分配。 */
    private final String symbol;

    private final OutboundRowHandler handler;
    private final MatchFill fillScratch = new MatchFill();
    private final AeronWire.EventFields eventScratch = new AeronWire.EventFields();
    private final OutboundDepth depthScratch = new OutboundDepth();
    private final IdleStrategy idle = new BackoffIdleStrategy(100, 100, 1_000, 1_000_000);

    /**
     * 分片重组（Aeron IPC MTU 默认 1408B）：超 MTU 消息（如 WAL 批量帧/大 FILL_BATCH）
     * 会被 Aeron 分成多个 fragment，裸 poll 会把分片误判为完整消息（分片首字节可能
     * 命中任意类型字节 → 批量 count 错乱 → 越界）。FragmentAssembler 重组后再回调
     * onFragment；单条小消息零开销直接转发。
     */
    private final FragmentAssembler assembler = new FragmentAssembler(this::onFragment);

    private volatile boolean running = true;

    public AeronOutboundSource(
            Subscription subscription, String symbol, OutboundRowHandler handler) {
        this.subscription = subscription;
        this.symbol = symbol;
        this.handler = handler;
    }

    /** 无预绑定 symbol 变体（测试/兼容）：解码走原路径。 */
    public AeronOutboundSource(Subscription subscription, OutboundRowHandler handler) {
        this(subscription, null, handler);
    }

    @Override
    public void run() {
        while (running) {
            int n = subscription.poll(assembler, FRAGMENT_LIMIT);
            if (n > 0) {
                handler.onBatchEnd();
            }
            if (n == 0) {
                idle.idle();
            }
        }
    }

    private void onFragment(
            DirectBuffer buffer, int offset, int length, io.aeron.logbuffer.Header header) {
        switch (buffer.getByte(offset)) {
            case AeronWire.TYPE_FILL -> {
                if (symbol != null) {
                    AeronWire.decodeFill(buffer, offset, fillScratch, symbol);
                } else {
                    AeronWire.decodeFill(buffer, offset, fillScratch);
                }
                handler.onFill(fillScratch);
            }
            case AeronWire.TYPE_FILL_BATCH -> {
                int count = buffer.getInt(offset + 1);
                int fillOffset = offset + 5;
                for (int i = 0; i < count; i++) {
                    if (symbol != null) {
                        AeronWire.decodeFill(buffer, fillOffset, fillScratch, symbol);
                    } else {
                        AeronWire.decodeFill(buffer, fillOffset, fillScratch);
                    }
                    handler.onFill(fillScratch);
                    fillOffset += 96;
                }
            }
            case AeronWire.TYPE_DEPTH -> {
                if (symbol != null) {
                    AeronWire.decodeDepth(buffer, offset, depthScratch, symbol);
                } else {
                    AeronWire.decodeDepth(buffer, offset, depthScratch);
                }
                handler.onDepth(depthScratch);
            }
            case AeronWire.TYPE_EVENT -> {
                if (symbol != null) {
                    AeronWire.decodeEvent(buffer, offset, eventScratch, symbol);
                } else {
                    AeronWire.decodeEvent(buffer, offset, eventScratch);
                }
                handler.onEvent(eventScratch.symbol, eventScratch.event, eventScratch.detail);
            }
            case AeronWire.TYPE_METRICS -> {
                MetricsSnapshot snapshot = AeronWire.decodeMetrics(buffer, offset);
                handler.onMetrics(snapshot);
            }
            default -> {
                // 未知类型：跳过（版本兼容，不做处理）。
            }
        }
    }

    public void stop() {
        running = false;
    }
}
