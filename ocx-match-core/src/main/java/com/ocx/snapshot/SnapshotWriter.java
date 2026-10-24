package com.ocx.snapshot;

import com.ocx.aeron.OcxAeron;
import com.ocx.aeron.StreamIds;
import com.ocx.codec.sbe.wal.MessageHeaderEncoder;
import com.ocx.codec.sbe.wal.OrderCreateEncoder;
import com.ocx.outbound.MatchEventSink;
import com.ocx.recovery.RecoveryConfig;

import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 快照录制线程（2026-08-30，快照迁往 Aeron Archive recording；Chronicle 已删）。
 *
 * <p>全局单线程消费各 symbol 撮合线程批末冻结的 {@link SnapshotFreeze}：把在簿订单/
 * 条件单编码为 OCX1 批量帧发布到 snapshot 流（每 snapshot 一次
 * {@code startRecording}/{@code stopRecording}，录制落在独立 Archive 进程），
 * 并解析入站锚点（min(撮合 image 位置, recorder 已录位置)）、回收旧 snapshot
 * recording（保留最近 2 份）与入站 journal 锚点前段。撮合线程只付冻结成本，
 * 全部 Archive 控制请求复用本线程一条常驻连接（消除原每次快照新建连接的抖动）。</p>
 */
public final class SnapshotWriter implements AutoCloseable {

    /** 发布 offer 超时（Archive 落盘过慢/断连时放弃本次快照并上报；录制共享线程
     *  饿快照会话时 240MB 快照可拖 200s+，2s 快速跳过，恢复锚点回退上一份）。 */
    private static final long OFFER_TIMEOUT_MS = 2_000L;

    private static final long IDLE_SLEEP_MS = 1L;

    /** journal 保留兜底节拍：默认 15s（独立线程；收紧保留窗减小瞬时磁盘峰值）。 */
    private static final long RETENTION_TICK_MS = 15_000L;

    private final OcxAeron aeron;
    private final RecoveryConfig config;
    private final String aeronDirectoryName;
    private final MatchEventSink eventSink;
    private final List<SymbolSink> sinks = new ArrayList<>();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Thread thread;

    /** journal 保留兜底独立线程（2026-09-02）：快照线程被慢速录制拖住时仍按时回收）。 */
    private Thread retentionThread;

    /** 快照段序号（快照线程单线程自增，2026-08-31 常驻 recording 分段）。 */
    private long snapshotSeq;

    /** 常驻快照 recording 描述（首次写快照时缓存；recording 不轮转）。 */
    private long snapshotRecordingId = -1L;

    private long snapshotStartPosition;
    private int snapshotSegmentFileLength;
    private int snapshotTermBufferLength;

    /** 快照段起点（publication position，BEGIN 发布前）——段级回收用。 */
    private final long[] segmentStarts = new long[16];

    private int segmentStartCount;

    /** 保留最近 N 段（对齐旧"保留 2 份"语义；恢复/对账取最新有效段）。 */
    private static final int KEEP_SNAPSHOT_SEGMENTS = 2;

    private AeronArchive archive;

    /** 保留线程独立 Archive client（2026-09-02：与快照线程共用 client 线程不安全，
     *  控制请求交错会静默失败——D10-D16 journal 无界/磁盘满的疑似根因之一）。 */
    private AeronArchive retentionArchive;

    /**
     * 每 symbol 快照交接点：撮合线程从 {@code freePool} 取空闲冻结缓冲 → 冻结 →
     * 放入 {@code ready}；快照线程取走处理完归还 {@code freePool}（双缓冲 ping-pong）。
     */
    public static final class SymbolSink {
        final String symbol;
        final AtomicReference<SnapshotFreeze> ready = new AtomicReference<>();
        final ArrayBlockingQueue<SnapshotFreeze> freePool = new ArrayBlockingQueue<>(2);
        final Publication publication;
        final UnsafeBuffer batch = new UnsafeBuffer(new byte[SnapshotCodec.MAX_BATCH_BYTES]);
        final UnsafeBuffer scratch =
                new UnsafeBuffer(
                        new byte[OrderRecordCodec.MAX_ENCODED_LENGTH + OrderRecordCodec.MAGIC_LEN]);
        final byte[] strScratch =
                new byte[OrderRecordCodec.MAX_ENCODED_LENGTH + OrderRecordCodec.MAGIC_LEN];
        final MessageHeaderEncoder header = new MessageHeaderEncoder();
        final OrderCreateEncoder enc = new OrderCreateEncoder();

        SymbolSink(String symbol, Publication publication) {
            this.symbol = symbol;
            this.publication = publication;
            freePool.offer(new SnapshotFreeze());
            freePool.offer(new SnapshotFreeze());
        }

        /** 撮合线程：取空闲冻结缓冲；快照线程尚未归还时返回 null（本次跳过，下批重试）。 */
        public SnapshotFreeze acquireFreeze() {
            return freePool.poll();
        }

        void returnFreeze(SnapshotFreeze freeze) {
            freePool.offer(freeze);
        }
    }

    /** 入站锚点解析结果（快照 META + 段回收用）。 */
    private record InboundAnchor(
            long recordingId,
            long position,
            long startPosition,
            int segmentFileLength,
            int termBufferLength,
            List<Long> retiredRecordings) {}

    public SnapshotWriter(
            OcxAeron aeron,
            RecoveryConfig config,
            String aeronDirectoryName,
            MatchEventSink eventSink) {
        this.aeron = aeron;
        this.config = config;
        this.aeronDirectoryName = aeronDirectoryName;
        this.eventSink = eventSink;
        this.thread = new Thread(this::run, "snapshot-writer");
        this.thread.setDaemon(true);
        this.retentionThread = new Thread(this::retentionLoop, "snapshot-retention");
        this.retentionThread.setDaemon(true);
    }

    /** 注册 symbol（创建 snapshot publication + 双缓冲交接点），组合根启动期调用。 */
    public SymbolSink register(String symbol) {
        SymbolSink sink = new SymbolSink(symbol, aeron.addSnapshotPublication(symbol));
        sinks.add(sink);
        return sink;
    }

    public void start() {
        archive =
                AeronArchive.connect(
                        new AeronArchive.Context()
                                .aeronDirectoryName(aeronDirectoryName)
                                .controlRequestChannel(config.getArchiveControlChannel())
                                .controlResponseChannel(config.getArchiveControlResponseChannel()));
        thread.start();
        retentionThread.start();
    }

    @Override
    public void close() {
        running.set(false);
        thread.interrupt();
        if (retentionThread != null) {
            retentionThread.interrupt();
            try {
                retentionThread.join(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (archive != null) {
            archive.close();
        }
    }

    /** 保留兜底循环：独立于快照写入（快照被慢速录制拖住不影响 journal 回收）。 */
    private void retentionLoop() {
        while (running.get()) {
            try {
                if (retentionArchive == null) {
                    retentionArchive =
                            AeronArchive.connect(
                                    new AeronArchive.Context()
                                            .aeronDirectoryName(aeronDirectoryName)
                                            .controlRequestChannel(
                                                    config.getArchiveControlChannel())
                                            .controlResponseChannel(
                                                    config.getArchiveControlResponseChannel()));
                }
                for (SymbolSink sink : sinks) {
                    purgeJournalRetention(retentionArchive, sink.symbol);
                }
            } catch (Throwable ignored) {
                // 瞬时故障下次重试
            }
            try {
                long tick = Long.getLong("ocx.journal.retain-tick-ms", RETENTION_TICK_MS);
                Thread.sleep(tick);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void run() {
        while (running.get()) {
            boolean worked = false;
            for (SymbolSink sink : sinks) {
                SnapshotFreeze freeze = sink.ready.getAndSet(null);
                if (freeze == null) {
                    continue;
                }
                try {
                    writeSnapshot(sink, freeze);
                } catch (Exception e) {
                    emit(sink.symbol, "snapshot_write_failed", String.valueOf(e.getMessage()));
                } finally {
                    growIfNeeded(sink, freeze);
                    sink.returnFreeze(freeze);
                }
                worked = true;
            }
            if (!worked) {
                try {
                    Thread.sleep(IDLE_SLEEP_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    /**
     * journal 保留兜底（2026-09-01 录制解耦）：独立于快照锚点，按
     * {@code -Docx.journal.retain-bytes}（默认 32GB）保留最新录制段，
     * 防止耐久/快照门控异常时 journal 无界增长撑满磁盘。
     */
    private void purgeJournalRetention(AeronArchive archiveClient, String symbol) {
        int streamId = StreamIds.inboundJournalEffective(symbol);
        long retainBytes = Long.getLong("ocx.journal.retain-bytes", 16L << 30);
        final long[] rec = {-1L, -1L, -1L, -1L, -1L};
        try {
            archiveClient.listRecordings(
                    0L,
                    Integer.MAX_VALUE,
                    (cs,
                            corr,
                            recordingId,
                            startTs,
                            triggerTs,
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
                        if (sid == streamId && recordingId > rec[0]) {
                            rec[0] = recordingId;
                            rec[1] =
                                    stopPosition == AeronArchive.NULL_POSITION
                                            ? archiveClient.getRecordingPosition(recordingId)
                                            : stopPosition;
                            rec[2] = startPosition;
                            rec[3] = segmentFileLength;
                            rec[4] = termBufferLength;
                        }
                    });
        } catch (Exception e) {
            return;
        }
        if (rec[0] < 0L || rec[1] < 0L || rec[3] <= 0 || rec[4] <= 0) {
            return;
        }
        long purgePos =
                AeronArchive.segmentFileBasePosition(
                        rec[2], Math.max(rec[2], rec[1] - retainBytes), (int) rec[4], (int) rec[3]);
        long firstBase =
                AeronArchive.segmentFileBasePosition(rec[2], rec[2], (int) rec[4], (int) rec[3]);
        if (purgePos > firstBase) {
            try {
                archiveClient.purgeSegments(rec[0], purgePos);
            } catch (Exception e) {
                System.err.println(
                        "[snapshot-retention] purge_journal_retention_failed"
                                + " recordingId="
                                + rec[0]
                                + " "
                                + e.getMessage());
            }
        }
    }

    private void writeSnapshot(SymbolSink sink, SnapshotFreeze freeze) throws Exception {
        InboundAnchor anchor = resolveInboundAnchor(sink.symbol, freeze);
        // 常驻 recording 分段（2026-08-31 方案 A）：快照流由 Archive 常驻录制，
        // 每张快照 = BEGIN(seq)+META+ORDER+STOP+END；不再 start/stop recording
        // （运行中临时 startRecording 踩 Aeron available-image 回调时序，recording
        // 不创建——专项定位）。异常段无 END，恢复/对账端弃用回退。
        long seq = snapshotSeq++;
        long segStart = sink.publication.position();
        int len = SnapshotCodec.encodeBegin(sink.batch, 0, seq);
        offer(sink.publication, sink.batch, 0, len);
        SnapshotCodec.SnapshotMeta meta =
                new SnapshotCodec.SnapshotMeta(
                        sink.symbol,
                        freeze.freezeTs,
                        freeze.coinScale,
                        freeze.baseCoinScale,
                        freeze.orderCount,
                        freeze.triggerCount,
                        anchor.recordingId(),
                        anchor.position());
        len = SnapshotCodec.encodeMeta(sink.batch, 0, meta);
        offer(sink.publication, sink.batch, 0, len);
        publishOrderBatches(sink, freeze);
        publishStopBatches(sink, freeze);
        len = SnapshotCodec.encodeEnd(sink.batch, 0, freeze.orderCount, freeze.triggerCount);
        offer(sink.publication, sink.batch, 0, len);
        emit(
                sink.symbol,
                "snapshot_written",
                "seq=" + seq + " orders=" + freeze.orderCount + " triggers=" + freeze.triggerCount);
        recordSegmentStart(segStart);
        purgeOldSnapshotSegments(sink.symbol);
        purgeInboundSegments(sink.symbol, anchor);
    }

    private void recordSegmentStart(long position) {
        if (segmentStartCount == segmentStarts.length) {
            System.arraycopy(
                    segmentStarts,
                    segmentStartCount - KEEP_SNAPSHOT_SEGMENTS,
                    segmentStarts,
                    0,
                    KEEP_SNAPSHOT_SEGMENTS);
            segmentStartCount = KEEP_SNAPSHOT_SEGMENTS;
        }
        segmentStarts[segmentStartCount++] = position;
    }

    /**
     * 快照段级回收（2026-08-31）：保留最近 {@link #KEEP_SNAPSHOT_SEGMENTS} 段，
     * 用 {@code purgeSegments} 删除最老保留段起点之前的段（对齐段边界，
     * 与入站 journal 锚点段回收同套路）。
     */
    private void purgeOldSnapshotSegments(String symbol) {
        if (segmentStartCount <= KEEP_SNAPSHOT_SEGMENTS) {
            return;
        }
        ensureSnapshotRecording(symbol);
        if (snapshotRecordingId < 0L
                || snapshotSegmentFileLength <= 0
                || snapshotTermBufferLength <= 0) {
            return;
        }
        long oldestKeep = segmentStarts[segmentStartCount - KEEP_SNAPSHOT_SEGMENTS];
        try {
            long purgePos =
                    AeronArchive.segmentFileBasePosition(
                            snapshotStartPosition,
                            oldestKeep,
                            snapshotTermBufferLength,
                            snapshotSegmentFileLength);
            long firstSegmentBase =
                    AeronArchive.segmentFileBasePosition(
                            snapshotStartPosition,
                            snapshotStartPosition,
                            snapshotTermBufferLength,
                            snapshotSegmentFileLength);
            if (purgePos > firstSegmentBase) {
                archive.purgeSegments(snapshotRecordingId, purgePos);
                emit(
                        symbol,
                        "purge_snapshot_segments",
                        "recordingId=" + snapshotRecordingId + " position=" + purgePos);
                System.arraycopy(
                        segmentStarts,
                        segmentStartCount - KEEP_SNAPSHOT_SEGMENTS,
                        segmentStarts,
                        0,
                        KEEP_SNAPSHOT_SEGMENTS);
                segmentStartCount = KEEP_SNAPSHOT_SEGMENTS;
            }
        } catch (Exception e) {
            emit(
                    symbol,
                    "purge_snapshot_segments_failed",
                    "recordingId=" + snapshotRecordingId + " " + e.getMessage());
        }
    }

    /** 缓存常驻快照 recording 的 descriptor（recording 不轮转，首次查询即可）。 */
    private void ensureSnapshotRecording(String symbol) {
        if (snapshotRecordingId >= 0L) {
            return;
        }
        int streamId = StreamIds.snapshot(symbol);
        archive.listRecordings(
                0L,
                Integer.MAX_VALUE,
                (cs,
                        corr,
                        recordingId,
                        startTs,
                        triggerTs,
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
                    if (sid == streamId && recordingId > snapshotRecordingId) {
                        snapshotRecordingId = recordingId;
                        snapshotStartPosition = startPosition;
                        snapshotSegmentFileLength = segmentFileLength;
                        snapshotTermBufferLength = termBufferLength;
                    }
                });
    }

    private void publishOrderBatches(SymbolSink sink, SnapshotFreeze f) throws Exception {
        int batchCount = 0;
        int len = SnapshotCodec.encodeBatchHeader(sink.batch, 0, SnapshotCodec.TYPE_ORDER_BATCH, 0);
        for (int i = 0; i < f.orderCount; i++) {
            int recLen =
                    OrderRecordCodec.encodeFields(
                            sink.symbol,
                            f.orderIds[i],
                            f.userIds[i],
                            f.prices[i],
                            f.origQtys[i],
                            f.dirs[i],
                            f.tifs[i],
                            f.times[i],
                            f.execQtys[i],
                            f.cumQuoteQtys[i],
                            sink.scratch,
                            0,
                            sink.strScratch,
                            sink.header,
                            sink.enc);
            if (recLen <= 0) {
                continue;
            }
            int next = SnapshotCodec.appendRecord(sink.batch, 0, len, sink.scratch, 0, recLen);
            if (next < 0) {
                SnapshotCodec.encodeBatchHeader(
                        sink.batch, 0, SnapshotCodec.TYPE_ORDER_BATCH, batchCount);
                offer(sink.publication, sink.batch, 0, len);
                len =
                        SnapshotCodec.encodeBatchHeader(
                                sink.batch, 0, SnapshotCodec.TYPE_ORDER_BATCH, 0);
                batchCount = 0;
                next = SnapshotCodec.appendRecord(sink.batch, 0, len, sink.scratch, 0, recLen);
            }
            len = next;
            batchCount++;
        }
        flushBatch(sink, SnapshotCodec.TYPE_ORDER_BATCH, batchCount, len);
    }

    private void publishStopBatches(SymbolSink sink, SnapshotFreeze f) throws Exception {
        int batchCount = 0;
        int len = SnapshotCodec.encodeBatchHeader(sink.batch, 0, SnapshotCodec.TYPE_STOP_BATCH, 0);
        for (int i = 0; i < f.triggerCount; i++) {
            int recLen =
                    OrderRecordCodec.encodeStopFields(
                            sink.symbol,
                            f.triggerIds[i],
                            f.triggerUserIds[i],
                            f.triggerPrices[i],
                            f.triggerLimitPrices[i],
                            f.triggerQtys[i],
                            f.triggerDirs[i],
                            f.triggerTypes[i],
                            f.triggerTrailDeltas[i],
                            f.triggerTimes[i],
                            sink.scratch,
                            0,
                            sink.strScratch,
                            sink.header,
                            sink.enc);
            if (recLen <= 0) {
                continue;
            }
            int next = SnapshotCodec.appendRecord(sink.batch, 0, len, sink.scratch, 0, recLen);
            if (next < 0) {
                SnapshotCodec.encodeBatchHeader(
                        sink.batch, 0, SnapshotCodec.TYPE_STOP_BATCH, batchCount);
                offer(sink.publication, sink.batch, 0, len);
                len =
                        SnapshotCodec.encodeBatchHeader(
                                sink.batch, 0, SnapshotCodec.TYPE_STOP_BATCH, 0);
                batchCount = 0;
                next = SnapshotCodec.appendRecord(sink.batch, 0, len, sink.scratch, 0, recLen);
            }
            len = next;
            batchCount++;
        }
        flushBatch(sink, SnapshotCodec.TYPE_STOP_BATCH, batchCount, len);
    }

    private void flushBatch(SymbolSink sink, int type, int batchCount, int len) throws Exception {
        if (batchCount <= 0) {
            return;
        }
        SnapshotCodec.encodeBatchHeader(sink.batch, 0, type, batchCount);
        offer(sink.publication, sink.batch, 0, len);
    }

    private InboundAnchor resolveInboundAnchor(String symbol, SnapshotFreeze freeze) {
        boolean bridgeMode =
                Boolean.parseBoolean(System.getProperty("ocx.journal-bridge.enabled", "false"));
        if (bridgeMode) {
            // 桥模式：锚点 = 冻结行（maxImage）对应的 journal 位置（精确到行，
            // 消除 durableJournalPos 超前冻结行造成的恢复缺口——D18 对账 U5/U1
            // 锚点边界违例根因）；查询失败回退 durableJournalPos
            long[] anchor = freeze.journalAnchor;
            long journalPos =
                    freeze.maxImagePos >= 0L
                            ? resolveBridgeJournalPos(symbol, freeze.maxImagePos)
                            : -1L;
            if (journalPos < 0L && anchor != null) {
                journalPos = anchor[1];
            }
            if (journalPos < 0L) {
                return new InboundAnchor(-1L, -1L, -1L, -1, -1, List.of());
            }
            int streamId = StreamIds.inboundJournal(symbol);
            long[] latest = {-1L, -1L, -1L, -1L, -1L, -1L};
            List<long[]> inboundRecordings = new ArrayList<>();
            archive.listRecordings(
                    0L,
                    Integer.MAX_VALUE,
                    (cs,
                            corr,
                            recordingId,
                            startTs,
                            triggerTs,
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
                        if (sid == streamId && recordingId > latest[0]) {
                            latest[0] = recordingId;
                            latest[1] = sessionId;
                            latest[2] = stopPosition;
                            latest[3] = segmentFileLength;
                            latest[4] = termBufferLength;
                            latest[5] = startPosition;
                        }
                        if (sid == streamId) {
                            inboundRecordings.add(new long[] {recordingId, stopPosition});
                        }
                    });
            if (latest[0] < 0L) {
                return new InboundAnchor(-1L, -1L, -1L, -1, -1, List.of());
            }
            List<Long> retired = new ArrayList<>();
            for (long[] rec : inboundRecordings) {
                if (rec[0] < latest[0] && rec[1] != AeronArchive.NULL_POSITION) {
                    retired.add(rec[0]);
                }
            }
            return new InboundAnchor(
                    latest[0], journalPos, latest[5], (int) latest[3], (int) latest[4], retired);
        }
        int streamId = StreamIds.inbound(symbol);
        // latest inbound recording：recordingId, sessionId, stopPosition, segmentFileLength,
        // termBufferLength, startPosition
        long[] latest = {-1L, -1L, -1L, -1L, -1L, -1L};
        List<long[]> inboundRecordings = new ArrayList<>();
        archive.listRecordings(
                0L,
                Integer.MAX_VALUE,
                (cs,
                        corr,
                        recordingId,
                        startTs,
                        triggerTs,
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
                    if (sid == streamId && recordingId > latest[0]) {
                        latest[0] = recordingId;
                        latest[1] = sessionId;
                        latest[2] = stopPosition;
                        latest[3] = segmentFileLength;
                        latest[4] = termBufferLength;
                        latest[5] = startPosition;
                    }
                    if (sid == streamId) {
                        inboundRecordings.add(new long[] {recordingId, stopPosition});
                    }
                });
        if (latest[0] < 0L) {
            return new InboundAnchor(-1L, -1L, -1L, -1, -1, List.of());
        }
        long recorderPos =
                latest[2] == AeronArchive.NULL_POSITION
                        ? archive.getRecordingPosition(latest[0])
                        : latest[2];
        Long imgPos =
                freeze.imagePositions == null ? null : freeze.imagePositions.get((int) latest[1]);
        long position =
                (imgPos != null && imgPos >= 0L && recorderPos >= 0L)
                        ? Math.min(imgPos, recorderPos)
                        : recorderPos;
        List<Long> retired = new ArrayList<>();
        for (long[] rec : inboundRecordings) {
            if (rec[0] < latest[0] && rec[1] != AeronArchive.NULL_POSITION) {
                retired.add(rec[0]);
            }
        }
        return new InboundAnchor(
                latest[0], position, latest[5], (int) latest[3], (int) latest[4], retired);
    }

    /**
     * 桥锚点精确查询（2026-09-02）：发 [maxImage] 给 bridge，等回 [livePos][journalPos]。
     * 60s 一次，ms 级往返；超时返回 -1（调用方回退 durableJournalPos）。
     */
    private long resolveBridgeJournalPos(String symbol, long livePos) {
        final UnsafeBuffer q = new UnsafeBuffer(new byte[Long.BYTES]);
        q.putLong(0, livePos);
        final long[] out = {-1L};
        try (Publication queryPub = aeron.addBridgeAnchorQueryPublication(symbol);
                Subscription replySub = aeron.addBridgeAnchorReplySubscription(symbol)) {
            FragmentAssembler assembler =
                    new FragmentAssembler(
                            (buffer, offset, length, header) -> {
                                if (length >= 16 && buffer.getLong(offset) == livePos) {
                                    out[0] = buffer.getLong(offset + Long.BYTES);
                                }
                            });
            IdleStrategy idle = new BackoffIdleStrategy(100, 100, 1_000, 1_000_000);
            long deadline = System.currentTimeMillis() + 2_000L;
            while (queryPub.offer(q, 0, Long.BYTES) < 0L && System.currentTimeMillis() < deadline) {
                idle.idle();
            }
            while (out[0] < 0L && System.currentTimeMillis() < deadline) {
                replySub.poll(assembler, 8);
                idle.idle();
            }
            return out[0];
        } catch (Exception e) {
            return -1L;
        }
    }

    /** 入站 journal 锚点前回收：旧 recording 整条删，锚点 recording 删锚点前段（对齐段边界）。 */
    private void purgeInboundSegments(String symbol, InboundAnchor anchor) {
        for (long id : anchor.retiredRecordings()) {
            try {
                archive.purgeRecording(id);
                emit(symbol, "purge_recording", "recordingId=" + id);
            } catch (Exception e) {
                emit(symbol, "purge_recording_failed", "recordingId=" + id + " " + e.getMessage());
            }
        }
        long recordingId = anchor.recordingId();
        long position = anchor.position();
        int segmentFileLength = anchor.segmentFileLength();
        int termBufferLength = anchor.termBufferLength();
        long startPosition = anchor.startPosition();
        if (recordingId < 0L || position < 0L || segmentFileLength <= 0 || termBufferLength <= 0) {
            return;
        }
        try {
            long anchorSegmentBase =
                    AeronArchive.segmentFileBasePosition(
                            startPosition, position, termBufferLength, segmentFileLength);
            long firstSegmentBase =
                    AeronArchive.segmentFileBasePosition(
                            startPosition, startPosition, termBufferLength, segmentFileLength);
            if (anchorSegmentBase > firstSegmentBase) {
                archive.purgeSegments(recordingId, anchorSegmentBase);
                emit(
                        symbol,
                        "purge_segments",
                        "recordingId=" + recordingId + " position=" + anchorSegmentBase);
            }
        } catch (Exception e) {
            emit(
                    symbol,
                    "purge_segments_failed",
                    "recordingId=" + recordingId + " " + e.getMessage());
        }
    }

    /** 快照线程在两次快照之间按上一轮在簿数预扩（80% 阈值），扩容不在撮合线程。 */
    private static void growIfNeeded(SymbolSink sink, SnapshotFreeze freeze) {
        int orders = freeze.orderCount;
        int triggers = freeze.triggerCount;
        if (orders > freeze.orderIds.length * 8 / 10) {
            freeze.ensureOrders(Math.max(orders * 2, freeze.orderIds.length));
        }
        if (triggers > freeze.triggerIds.length * 8 / 10) {
            freeze.ensureStops(Math.max(triggers * 2, freeze.triggerIds.length));
        }
    }

    private static void offer(
            Publication publication, MutableDirectBuffer buffer, int offset, int length)
            throws Exception {
        long deadline = System.currentTimeMillis() + OFFER_TIMEOUT_MS;
        while (publication.offer(buffer, offset, length) < 0L) {
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException(
                        "快照发布超时: stream=" + publication.streamId() + " bytes=" + length);
            }
            Thread.onSpinWait();
        }
    }

    private void emit(String symbol, String event, String detail) {
        if (eventSink != null) {
            eventSink.emit(symbol, event, detail);
        }
    }
}
