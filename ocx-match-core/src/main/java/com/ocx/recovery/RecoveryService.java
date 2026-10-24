package com.ocx.recovery;

import com.ocx.aeron.AeronWire;
import com.ocx.aeron.OcxAeron;
import com.ocx.aeron.StreamIds;
import com.ocx.book.OrderBook;
import com.ocx.book.OrderRequest;
import com.ocx.book.TriggerSlots;
import com.ocx.codec.IoCodec;
import com.ocx.codec.IoCodec.CodecContext;
import com.ocx.codec.domain.OrderInFields;
import com.ocx.handler.OrderEventHandler;
import com.ocx.math.Decimal;
import com.ocx.outbound.MatchEventSink;
import com.ocx.snapshot.OrderRecordCodec;
import com.ocx.snapshot.SnapshotCodec;
import com.ocx.snapshot.SnapshotOrderFields;
import com.ocx.snapshot.TriggerSnapshotFields;

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * 恢复服务（2026-08-29 输入 journal 重构）——启动时从 Aeron Archive 的**入站 recording**
 * 回放输入帧，重跑完整撮合逻辑重建 OrderBook 状态（含停损单）。
 *
 * <p>对齐 LMAX/Aeron：journal 输入而非内部状态变更。入站流（{@link StreamIds#inbound(String)}）
 * 由 Archive 持续记录（OcxArchiveMain / 内嵌 Archive）；崩溃恢复 = 快照（若有，携带
 * 入站 recordingId+position 锚点）预填 + 从锚点增量回放入站帧，逐帧走完整撮合
 * （{@code OrderEventHandler.getEngine().onOrderIn}）。</p>
 *
 * <p>回放期间置位 replayMode：不收集成交（成交/深度恢复由各自 recording 承担，
 * 重建期间产生的成交不重复下发）。无快照时全量回放入站。</p>
 */
public class RecoveryService {

    /** 回放静默判定：无新消息超过该时长认为 recording 回放完毕。 */
    private static final long REPLAY_QUIET_MS = 2_000L;

    /** 回放总截止：超大 recording 全量回放的安全上限。 */
    private static final long REPLAY_DEADLINE_MS = 30 * 60_000L;

    /** 关键事件出口（组合根注入；null 时静默）。 */
    private final MatchEventSink eventSink;

    public RecoveryService() {
        this(null);
    }

    public RecoveryService(MatchEventSink eventSink) {
        this.eventSink = eventSink;
    }

    /** 恢复统计：入站帧重放计数 + 撮合结果粗分（按 onOrderIn 返回码）。 */
    public static final class WalRecoveryStats {
        public long records;
        public long created;
        public long cancelled;
        public long matched;
        public long triggered;
        public long skipped;

        void add(WalRecoveryStats other) {
            records += other.records;
            created += other.created;
            cancelled += other.cancelled;
            matched += other.matched;
            triggered += other.triggered;
            skipped += other.skipped;
        }

        @Override
        public String toString() {
            return "records="
                    + records
                    + " created="
                    + created
                    + " cancelled="
                    + cancelled
                    + " matched="
                    + matched
                    + " triggered="
                    + triggered
                    + " skipped="
                    + skipped;
        }
    }

    /** 单条入站 recording 摘要（回放用）。 */
    private record Recording(long recordingId, long startPosition, long stopPosition) {}

    /**
     * 纵表链路恢复：从 Archive 回放入站 recording，重跑撮合重建簿状态。
     *
     * @param config WAL 配置（enabled / 快照目录 / Archive 控制通道）
     * @param aeronDirectoryName 媒体驱动目录（与 OcxAeron.connect 同源）
     * @param handlers symbol → 处理器（提供 book/engine/triggers；回放期间置 replayMode）
     */
    public WalRecoveryStats recover(
            RecoveryConfig config,
            String aeronDirectoryName,
            Map<String, OrderEventHandler> handlers) {
        WalRecoveryStats total = new WalRecoveryStats();
        if (!config.isEnabled()) {
            return total;
        }
        try (AeronArchive archive =
                AeronArchive.connect(
                        new AeronArchive.Context()
                                .aeronDirectoryName(aeronDirectoryName)
                                .controlRequestChannel(config.getArchiveControlChannel())
                                .controlResponseChannel(
                                        config.getArchiveControlResponseChannel()))) {
            for (Map.Entry<String, OrderEventHandler> e : handlers.entrySet()) {
                String symbol = e.getKey();
                try {
                    total.add(replaySymbol(archive, config, symbol, e.getValue()));
                } catch (Exception ex) {
                    emit(symbol, "wal_recovery_failed", String.valueOf(ex.getMessage()));
                }
            }
        } catch (Exception e) {
            emit("", "wal_archive_unavailable", String.valueOf(e.getMessage()));
        }
        return total;
    }

    private WalRecoveryStats replaySymbol(
            AeronArchive archive, RecoveryConfig config, String symbol, OrderEventHandler handler) {
        WalRecoveryStats stats = new WalRecoveryStats();
        OrderBook ob = handler.getBook();

        // 快照预填（2026-08-30：Aeron Archive recording 回放，替代 Chronicle 快照目录）：
        // 逐份回放 snapshot recording，END 缺失/计数不符弃用，回退上一有效份；
        // 均无效返回 null（全量回放入站）。锚点 = META 内入站 recordingId+position。
        SnapshotCodec.SnapshotMeta loaded = loadAndApplySnapshot(archive, symbol, handler);
        long snapRecordingId = loaded != null ? loaded.inboundRecordingId() : -1L;
        long snapPosition = loaded != null ? loaded.inboundPosition() : -1L;

        List<Recording> recordings = listInboundRecordings(archive, symbol);
        if (Boolean.getBoolean("ocx.recovery.debug")) {
            System.err.println(
                    "[recovery] symbol="
                            + symbol
                            + " snapRecId="
                            + snapRecordingId
                            + " snapPos="
                            + snapPosition);
            for (Recording r : recordings) {
                System.err.println(
                        "[recovery] rec="
                                + r.recordingId()
                                + " start="
                                + r.startPosition()
                                + " stop="
                                + resolvedStop(archive, r));
            }
        }
        if (recordings.isEmpty()) {
            return stats;
        }
        boolean anchored = snapRecordingId >= 0L && snapPosition >= 0L;
        handler.setReplayMode(true);
        try {
            for (Recording rec : recordings) {
                long start = rec.startPosition();
                if (anchored) {
                    if (rec.recordingId() < snapRecordingId) {
                        continue; // 快照点之前的旧 recording 已由快照覆盖
                    }
                    if (rec.recordingId() == snapRecordingId) {
                        start = Math.max(start, snapPosition);
                    }
                }
                long stop = resolvedStop(archive, rec);
                if (stop <= start) {
                    continue;
                }
                stats.add(replayInbound(archive, rec.recordingId(), start, stop, symbol, handler));
            }
        } finally {
            handler.setReplayMode(false);
        }
        return stats;
    }

    /**
     * 加载并应用快照：按 recordingId 从新到旧逐份回放，取最新通过完整性校验者
     * （META + END + 计数一致）预填簿；更新的份校验失败时上报回退事件。
     *
     * @return 应用的快照 META（含入站锚点）；无有效快照返回 null（全量回放入站）
     */
    private SnapshotCodec.SnapshotMeta loadAndApplySnapshot(
            AeronArchive archive, String symbol, OrderEventHandler handler) {
        List<Recording> recordings = listSnapshotRecordings(archive, symbol);
        recordings.sort(Comparator.comparingLong(Recording::recordingId).reversed());
        boolean skippedNewer = false;
        for (Recording rec : recordings) {
            SnapshotVerify verify = verifySnapshotRecording(archive, rec, symbol);
            if (verify == null || !verify.valid()) {
                emit(
                        symbol,
                        "snapshot_load_failed",
                        "recordingId=" + rec.recordingId() + " 快照不完整（END/计数校验失败）");
                skippedNewer = true;
                continue;
            }
            if (skippedNewer) {
                emit(
                        symbol,
                        "snapshot_fallback_prev",
                        "更新快照不可用，回退 recordingId=" + rec.recordingId());
            }
            applySnapshotRecording(archive, rec, symbol, handler, verify);
            return verify.meta();
        }
        return null;
    }

    /** 列出 symbol 的 snapshot recording（快照流；每 snapshot 一次 start/stop recording）。 */
    private static List<Recording> listSnapshotRecordings(AeronArchive archive, String symbol) {
        int streamId = StreamIds.snapshot(symbol);
        List<Recording> list = new ArrayList<>();
        archive.listRecordings(
                0L,
                Integer.MAX_VALUE,
                (controlSessionId,
                        correlationId,
                        recordingId,
                        startTimestamp,
                        triggerTimestamp,
                        startPosition,
                        stopPosition,
                        initialTermId,
                        segmentFileLength,
                        termBufferLength,
                        mtuLength,
                        sessionId,
                        sid,
                        strippedChannel,
                        originalChannel,
                        sourceIdentity) -> {
                    if (sid == streamId) {
                        list.add(new Recording(recordingId, startPosition, stopPosition));
                    }
                });
        return list;
    }

    /** 校验结果：META + END + 计数一致才有效。 */
    private record SnapshotVerify(
            boolean valid, SnapshotCodec.SnapshotMeta meta, long beginMsgIndex) {}

    /** 回放 snapshot recording 校验完整性（META/END/计数；不落簿）。 */
    private SnapshotVerify verifySnapshotRecording(
            AeronArchive archive, Recording rec, String symbol) {
        // 常驻 recording 分段扫描（2026-08-31 方案 A）：逐段 BEGIN+META+ORDER+
        // STOP+END 校验，取"最新有效段"的 meta + BEGIN 消息序号（apply 按序号定位）。
        long[] bestBeginMsg = {-1L};
        SnapshotCodec.SnapshotMeta[] bestMeta = {null};
        long[] msgCount = {0L};
        long[] curBeginMsg = {-1L};
        boolean[] inSegment = {false};
        int[] orderCount = {0};
        int[] triggerCount = {0};
        boolean[] endSeen = {false};
        int[][] endCounts = {null};
        SnapshotCodec.SnapshotMeta[] curMeta = {null};
        try {
            replaySnapshotRecording(
                    archive,
                    rec,
                    symbol,
                    (buffer, offset, length) -> {
                        if (!SnapshotCodec.hasMagic(buffer, offset)) {
                            return;
                        }
                        msgCount[0]++;
                        switch (SnapshotCodec.type(buffer, offset)) {
                            case SnapshotCodec.TYPE_SNAPSHOT_BEGIN -> {
                                // 开新段：上一段若已有效已记录；重置当前段状态
                                inSegment[0] = true;
                                curBeginMsg[0] = msgCount[0];
                                orderCount[0] = 0;
                                triggerCount[0] = 0;
                                endSeen[0] = false;
                                endCounts[0] = null;
                                curMeta[0] = null;
                            }
                            case SnapshotCodec.TYPE_META -> {
                                if (inSegment[0]) {
                                    curMeta[0] = SnapshotCodec.decodeMeta(symbol, buffer, offset);
                                }
                            }
                            case SnapshotCodec.TYPE_ORDER_BATCH ->
                                    SnapshotCodec.forEachRecord(
                                            buffer,
                                            offset,
                                            length,
                                            (p, l) -> {
                                                if (inSegment[0]) {
                                                    orderCount[0]++;
                                                }
                                            });
                            case SnapshotCodec.TYPE_STOP_BATCH ->
                                    SnapshotCodec.forEachRecord(
                                            buffer,
                                            offset,
                                            length,
                                            (p, l) -> {
                                                if (inSegment[0]) {
                                                    triggerCount[0]++;
                                                }
                                            });
                            case SnapshotCodec.TYPE_END -> {
                                if (inSegment[0] && curMeta[0] != null) {
                                    endSeen[0] = true;
                                    endCounts[0] = SnapshotCodec.decodeEndCounts(buffer, offset);
                                    boolean valid =
                                            endSeen[0]
                                                    && endCounts[0] != null
                                                    && endCounts[0][0] == curMeta[0].count()
                                                    && endCounts[0][1] == curMeta[0].triggerCount()
                                                    && orderCount[0] == curMeta[0].count()
                                                    && triggerCount[0] == curMeta[0].triggerCount();
                                    if (valid) {
                                        bestBeginMsg[0] = curBeginMsg[0];
                                        bestMeta[0] = curMeta[0];
                                    }
                                }
                            }
                            default -> {}
                        }
                    });
        } catch (Exception e) {
            return new SnapshotVerify(false, null, -1L);
        }
        return new SnapshotVerify(bestMeta[0] != null, bestMeta[0], bestBeginMsg[0]);
    }

    /** 回放 snapshot recording 预填簿（已通过 {@link #verifySnapshotRecording} 的份）。 */
    private void applySnapshotRecording(
            AeronArchive archive,
            Recording rec,
            String symbol,
            OrderEventHandler handler,
            SnapshotVerify verify) {
        OrderBook ob = handler.getBook();
        TriggerSlots triggers = handler.getStops();
        // 常驻 recording：按"最新有效段 BEGIN 消息序号"定位，收集该段 ORDER/STOP 预填
        long[] msgCount = {0L};
        boolean[] collecting = {false};
        long[] orderDecodeFail = {0L};
        long[] triggerDecodeFail = {0L};
        replaySnapshotRecording(
                archive,
                rec,
                symbol,
                (buffer, offset, length) -> {
                    if (!SnapshotCodec.hasMagic(buffer, offset)) {
                        return;
                    }
                    msgCount[0]++;
                    switch (SnapshotCodec.type(buffer, offset)) {
                        case SnapshotCodec.TYPE_SNAPSHOT_BEGIN ->
                                collecting[0] = msgCount[0] == verify.beginMsgIndex();
                        case SnapshotCodec.TYPE_ORDER_BATCH ->
                                SnapshotCodec.forEachRecord(
                                        buffer,
                                        offset,
                                        length,
                                        (p, l) -> {
                                            if (!collecting[0]) {
                                                return;
                                            }
                                            byte[] bin = new byte[l];
                                            buffer.getBytes(p, bin);
                                            try {
                                                SnapshotOrderFields f =
                                                        OrderRecordCodec.decodeFields(bin);
                                                ob.addOrderDirect(toRequest(f, ob), f.time);
                                            } catch (Exception e) {
                                                // 单条损坏：跳过（与旧 Chronicle 快照语义一致）；
                                                // 2026-09-01 起计数上报——2026-08-31 曾因静默吞异常
                                                // 让快照预填整体失效而不自知（对账 U5 尾部根因）
                                                orderDecodeFail[0]++;
                                                if (orderDecodeFail[0] <= 3) {
                                                    emit(
                                                            symbol,
                                                            "snapshot_order_decode_failed",
                                                            "recLen=" + l + " err=" + e);
                                                }
                                            }
                                        });
                        case SnapshotCodec.TYPE_STOP_BATCH ->
                                SnapshotCodec.forEachRecord(
                                        buffer,
                                        offset,
                                        length,
                                        (p, l) -> {
                                            if (!collecting[0]) {
                                                return;
                                            }
                                            byte[] bin = new byte[l];
                                            buffer.getBytes(p, bin);
                                            try {
                                                TriggerSnapshotFields f =
                                                        OrderRecordCodec.decodeStopFields(bin);
                                                if (triggers != null) {
                                                    int rc =
                                                            triggers.add(
                                                                    f.orderId(),
                                                                    f.userId(),
                                                                    f.triggerPrice(),
                                                                    f.price(),
                                                                    f.origQty(),
                                                                    f.direction(),
                                                                    f.triggerType(),
                                                                    f.trailDelta(),
                                                                    f.time());
                                                    if (rc != 0) {
                                                        emit(
                                                                symbol,
                                                                "snapshot_stop_restore_failed",
                                                                "rc="
                                                                        + rc
                                                                        + " orderId="
                                                                        + f.orderId());
                                                    }
                                                }
                                            } catch (Exception e) {
                                                triggerDecodeFail[0]++;
                                                if (triggerDecodeFail[0] <= 3) {
                                                    emit(
                                                            symbol,
                                                            "snapshot_stop_decode_failed",
                                                            "recLen=" + l + " err=" + e);
                                                }
                                            }
                                        });
                        default -> {}
                    }
                });
        if (orderDecodeFail[0] > 0 || triggerDecodeFail[0] > 0) {
            emit(
                    symbol,
                    "snapshot_prefill_decode_failed",
                    "orderFail=" + orderDecodeFail[0] + " triggerFail=" + triggerDecodeFail[0]);
        }
    }

    /** 回放 snapshot recording 的逐条消息（META/批量帧/END），静默/截止语义与入站回放一致。 */
    private void replaySnapshotRecording(
            AeronArchive archive, Recording rec, String symbol, SnapshotMessageConsumer consumer) {
        int replayStream = StreamIds.snapshotReplay(symbol);
        FragmentAssembler assembler =
                new FragmentAssembler(
                        (buffer, offset, length, header) ->
                                consumer.accept(buffer, offset, length));
        long stop = resolvedStop(archive, rec);
        if (stop <= rec.startPosition()) {
            return;
        }
        try (Subscription sub =
                archive.replay(
                        rec.recordingId(),
                        rec.startPosition(),
                        stop - rec.startPosition(),
                        OcxAeron.CHANNEL,
                        replayStream)) {
            IdleStrategy idle = new BackoffIdleStrategy(100, 100, 1_000, 1_000_000);
            long lastActivity = System.currentTimeMillis();
            long start = System.currentTimeMillis();
            while (true) {
                int n = sub.poll(assembler, 256);
                if (n > 0) {
                    lastActivity = System.currentTimeMillis();
                    continue;
                }
                if (System.currentTimeMillis() - lastActivity > REPLAY_QUIET_MS
                        || System.currentTimeMillis() - start > REPLAY_DEADLINE_MS) {
                    break;
                }
                idle.idle();
            }
        }
    }

    @FunctionalInterface
    private interface SnapshotMessageConsumer {
        void accept(DirectBuffer buffer, int offset, int length);
    }

    /** 列出 symbol 的入站 recording（按 recordingId 升序；连续记录通常只有一条）。 */
    private static List<Recording> listInboundRecordings(AeronArchive archive, String symbol) {
        int streamId = StreamIds.inboundJournalEffective(symbol);
        List<Recording> list = new ArrayList<>();
        archive.listRecordings(
                0L,
                Integer.MAX_VALUE,
                (controlSessionId,
                        correlationId,
                        recordingId,
                        startTimestamp,
                        triggerTimestamp,
                        startPosition,
                        stopPosition,
                        initialTermId,
                        segmentFileLength,
                        termBufferLength,
                        mtuLength,
                        sessionId,
                        sid,
                        strippedChannel,
                        originalChannel,
                        sourceIdentity) -> {
                    if (sid == streamId) {
                        list.add(new Recording(recordingId, startPosition, stopPosition));
                    }
                });
        list.sort(Comparator.comparingLong(Recording::recordingId));
        return list;
    }

    /** recording 未停止时 stopPosition 为 NULL_POSITION：回退到当前录制位置。 */
    private static long resolvedStop(AeronArchive archive, Recording rec) {
        long stop = rec.stopPosition();
        if (stop == AeronArchive.NULL_POSITION) {
            stop = archive.getRecordingPosition(rec.recordingId());
        }
        return stop;
    }

    /** 回放入站 recording：逐行（8B 收单时刻 + 变长 SBE 帧）走完整撮合重建簿状态。 */
    private WalRecoveryStats replayInbound(
            AeronArchive archive,
            long recordingId,
            long position,
            long stop,
            String symbol,
            OrderEventHandler handler) {
        WalRecoveryStats stats = new WalRecoveryStats();
        CodecContext ctx = IoCodec.context();
        OrderInFields fields = new OrderInFields();
        FragmentAssembler assembler =
                new FragmentAssembler(
                        (buffer, offset, length, header) -> {
                            if (length < AeronWire.INBOUND_TS_LENGTH) {
                                return;
                            }
                            long ts = AeronWire.readInboundTimestamp(buffer, offset);
                            IoCodec.readOrderIn(
                                    ctx,
                                    buffer,
                                    offset + AeronWire.INBOUND_TS_LENGTH,
                                    Math.min(
                                            length - AeronWire.INBOUND_TS_LENGTH,
                                            AeronWire.INBOUND_FRAME_LENGTH),
                                    fields);
                            try {
                                int rc = handler.getEngine().onOrderIn(fields, ts);
                                stats.records++;
                                switch (rc) {
                                    case 2, 4, 5 -> stats.cancelled++;
                                    default -> {}
                                }
                            } catch (Throwable t) {
                                if (Boolean.getBoolean("ocx.recovery.debug")) {
                                    System.err.println("[recovery] onOrderIn 异常: " + t);
                                    t.printStackTrace(System.err);
                                }
                                throw t;
                            }
                        });
        int replayStream = StreamIds.walReplay(symbol);
        try (Subscription sub =
                archive.replay(
                        recordingId, position, stop - position, OcxAeron.CHANNEL, replayStream)) {
            if (Boolean.getBoolean("ocx.recovery.debug")) {
                System.err.println(
                        "[recovery] replay rec="
                                + recordingId
                                + " pos="
                                + position
                                + " len="
                                + (stop - position)
                                + " stream="
                                + replayStream
                                + " images="
                                + sub.imageCount());
            }
            IdleStrategy idle = new BackoffIdleStrategy(100, 100, 1_000, 1_000_000);
            long lastActivity = System.currentTimeMillis();
            long start = System.currentTimeMillis();
            while (true) {
                int n = sub.poll(assembler, 256);
                if (Boolean.getBoolean("ocx.recovery.debug") && n > 0) {
                    System.err.println("[recovery] poll n=" + n);
                }
                if (n > 0) {
                    lastActivity = System.currentTimeMillis();
                    continue;
                }
                if (System.currentTimeMillis() - lastActivity > REPLAY_QUIET_MS
                        || System.currentTimeMillis() - start > REPLAY_DEADLINE_MS) {
                    break;
                }
                idle.idle();
            }
        }
        return stats;
    }

    /**
     * 快照字段 → 定点入簿请求。
     * 字段版（exponent=0，值已是定点）直接使用；旧文件（原始 exponent）按 symbol scale
     * 归一化（scaleMantissa 幂等，不会二次缩放）。
     */
    private OrderRequest toRequest(SnapshotOrderFields f, OrderBook ob) {
        OrderRequest r = new OrderRequest();
        r.orderId = f.orderId;
        r.userId = f.userId;
        r.direction = f.direction;
        r.price =
                f.priceExponent == 0
                        ? f.price
                        : Decimal.scaleMantissa(f.price, f.priceExponent, ob.coinScale());
        r.origQty =
                f.origQtyExponent == 0
                        ? f.origQty
                        : Decimal.scaleMantissa(f.origQty, f.origQtyExponent, ob.baseCoinScale());
        r.timeInForce = f.tif;
        r.time = f.time;
        return r;
    }

    private void emit(String symbol, String event, String detail) {
        if (eventSink != null) {
            eventSink.emit(symbol, event, detail);
        }
    }
}
