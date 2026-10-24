package com.ocx.snapshot;

import com.ocx.book.OrderBook;
import com.ocx.book.TriggerSlots;

import java.util.Map;
import java.util.function.Supplier;

/**
 * 快照协调器——冻结侧（2026-08-30，快照迁往 Aeron Archive recording）：由撮合线程
 * 批末（{@code OrderEventHandler.flushEndOfBatch}）低频触发，把在簿订单/条件单
 * 单遍冻结进 {@link SnapshotFreeze}（SoA 紧凑列，零分配），连同入站 image 位置快照
 * 交接给 {@link SnapshotWriter} 线程；编码/发布/落盘/回收全部离开撮合线程。
 *
 * <p>锚点语义不变：{@code position = min(入站 image 消费字节位置, recorder 已录字节位置)}
 * ——二者同处入站流字节位置空间（Aeron position，跨 term 单调递增）。image 位置在
 * 撮合线程上采集（sessionId → 位置映射），快照线程按 recording sessionId 精确匹配。</p>
 *
 * <p>双缓冲 ping-pong：快照线程尚未归还冻结缓冲（处理慢于 interval）时本次跳过、
 * 下次批末重试（与原"Archive 慢则跳过"语义一致）。interval 由
 * {@code -Docx.snapshot.interval-ms} 调节（0=关闭快照）。</p>
 */
public final class SnapshotCoordinator {

    private final OrderBook book;
    private final TriggerSlots triggers;

    /** 冻结时刻入站 image 位置快照（sessionId → 字节位置；撮合线程上采集，线程安全）。 */
    private final Supplier<Map<Integer, Long>> imagePositions;

    private final long intervalMs;
    private final SnapshotWriter.SymbolSink sink;

    /** 桥模式 journal 耐久位置供给（{@code long[]{livePos, journalPos}}；null = 兼容模式）。 */
    private final Supplier<long[]> journalDurability;

    private long lastSnapshotMs = -1L;

    public SnapshotCoordinator(
            OrderBook book,
            TriggerSlots triggers,
            Supplier<Map<Integer, Long>> imagePositions,
            long intervalMs,
            SnapshotWriter.SymbolSink sink,
            Supplier<long[]> journalDurability) {
        this.book = book;
        this.triggers = triggers;
        this.imagePositions = imagePositions;
        this.intervalMs = intervalMs;
        this.sink = sink;
        this.journalDurability = journalDurability;
    }

    /** 撮合线程批末回调：到间隔则冻结并交接（不抛、不阻塞；快照线程忙则跳过）。
     *  @param lastInboundTsMs 本批最后入站帧 ts（2026-08-31：与成交 recording 的
     *  fill.time 同钟，快照 freezeTs 用它保证锚点边界对账无 ms 级时钟错位） */
    public void onBatchEnd(long lastInboundTsMs) {
        if (intervalMs <= 0L || lastInboundTsMs - lastSnapshotMs < intervalMs) {
            return;
        }
        SnapshotFreeze freeze = sink.acquireFreeze();
        if (freeze == null) {
            // 快照线程尚未归还缓冲（上一张仍在写）：跳过本次，下次批末重试
            lastSnapshotMs = -1L;
            return;
        }
        freeze.journalAnchor = null;
        if (journalDurability != null) {
            long[] d = journalDurability.get();
            if (d == null) {
                // 桥尚未上报耐久位置：跳过本次，下次批末重试
                sink.returnFreeze(freeze);
                lastSnapshotMs = -1L;
                return;
            }
            long maxImage = -1L;
            for (long v : imagePositions.get().values()) {
                maxImage = Math.max(maxImage, v);
            }
            if (d[0] < maxImage) {
                // journal 尚未耐久覆盖到 core 消费点：跳过（不产出不一致快照）
                sink.returnFreeze(freeze);
                lastSnapshotMs = -1L;
                return;
            }
            freeze.journalAnchor = d;
            freeze.maxImagePos = maxImage;
        }
        try {
            book.snapshotOrders(freeze);
            triggers.snapshotTriggers(freeze);
            freeze.freezeTs = lastInboundTsMs;
            freeze.imagePositions = imagePositions.get();
            sink.ready.set(freeze);
            lastSnapshotMs = lastInboundTsMs;
        } catch (Exception e) {
            sink.returnFreeze(freeze);
            lastSnapshotMs = -1L;
        }
    }
}
