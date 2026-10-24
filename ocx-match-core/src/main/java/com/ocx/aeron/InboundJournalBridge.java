package com.ocx.aeron;

import io.aeron.Aeron;
import io.aeron.Counter;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 入站录制有界解耦桥（2026-09-01，录制端解耦专项方案 1）。
 *
 * <p>拓扑：ingress 只发布 feed 流（{@link StreamIds#inboundFeed}，无录制端）；
 * 本桥（ingress 进程内单线程）订阅 feed → 写入环形缓冲 → 分别转发到
 * <b>live 流</b>（core 订阅，仅受 core 流控，录制卡顿不影响）与
 * <b>journal 流</b>（Archive 录制，受录制端流控）。录制端卡顿时环缓冲吸收，
 * feed 发布端只在缓冲耗尽时回压（有界缺口 = 环大小，可配）。</p>
 *
 * <p>快照锚点：桥每 ~50ms 经 Archive 查询 journal 录制位置，并沿环推进
 * "最新已耐久行"游标，把 {@code (durableLivePos, durableJournalPos)}
 * 写入媒体驱动计数器（label 前缀 {@code journal-durable-}），core 侧
 * SnapshotWriter/Coordinator 按 label 读取——快照仅在
 * {@code durableLivePos ≥ core 消费位置} 时冻结，锚点 = durableJournalPos。</p>
 *
 * <p>环行布局：{@code [4B len][8B liveEnd][8B journalEnd][row bytes]}，
 * 行不跨环边界（空间不足时等待释放）。liveEnd/journalEnd 由各自消费游标
 * offer 成功后回写，耐久扫描只读 journal 已发布行。</p>
 */
public final class InboundJournalBridge implements Runnable {

    /** 环行头：4B 行长 + 8B live 端位置 + 8B journal 端位置。 */
    private static final int ROW_HDR = 4 + 8 + 8;

    /** 每轮循环 feed/live/journal 处理预算（行数），保证三路公平。 */
    private static final int DRAIN_BUDGET = 4096;

    private static final int FEED_POLL_BUDGET = 4096;

    /** 稀疏索引：journal 消费每 K 行记一条 (ringOffset, journalEnd)，耐久扫描从索引起步。 */
    private static final int SPARSE_K = 1024;

    private static final int SPARSE_CAP = 8192;

    /** 行跨环边界时的消费侧拷贝 scratch（行上限 = 帧上限 256B + 头）。 */
    private static final int MAX_ROW = AeronWire.INBOUND_ROW_SIZE;

    private static final long RECORDER_POLL_INTERVAL_MS = 50L;
    private static final int CUSTOM_COUNTER_TYPE = 0xC0FFEE;

    private final Subscription feedSubscription;
    private final Publication livePublication;
    private final Publication journalPublication;
    private final Aeron aeron;
    private final String symbol;
    private final byte[] ring;
    private final UnsafeBuffer ringBuffer;
    private final byte[] rowScratch = new byte[MAX_ROW];
    private final UnsafeBuffer rowScratchBuffer = new UnsafeBuffer(rowScratch);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final IdleStrategy idle = new BackoffIdleStrategy(100, 100, 1_000, 1_000_000);

    /** 环游标（字节偏移；单线程消费，无需锁）。 */
    private volatile long head;

    private volatile long liveTail;
    private volatile long journalTail;

    /** 最新已耐久行的单调环游标（耐久线程独占；行不跨环边界）。 */
    private long durableCursor = -1L;

    /** 稀疏索引环（耐久线程读；journal 消费写）：[ringOffset, liveEnd, journalEnd] 三元组。 */
    private final long[] sparseIndex = new long[SPARSE_CAP * 3];

    private long sparseHead; // journal 消费线程独占：已写索引条数
    private long sparseScan; // 耐久线程独占：已扫描到的索引位置
    private long sparseCount; // journal 消费线程独占：实际索引条数（首条恒记）

    private volatile long durableLivePos = -1L;
    private volatile long durableJournalPos = -1L;
    private long recorderRecordingId = -1L;
    private AeronArchive archive;
    private Thread thread;
    private Thread durabilityThread;
    private final Counter durableLiveCounter;
    private final Counter durableJournalCounter;
    private final Counter fillCounter;

    /** 锚点查询/应答（core SnapshotWriter → bridge → core；快照锚点精确到冻结行）。 */
    private final Subscription anchorQuerySubscription;

    private final Publication anchorReplyPublication;
    private final UnsafeBuffer anchorScratch = new UnsafeBuffer(new byte[16]);
    private final FragmentHandler anchorQueryHandler = this::onAnchorQuery;

    public InboundJournalBridge(
            Aeron aeron,
            Subscription feedSubscription,
            Publication livePublication,
            Publication journalPublication,
            String symbol,
            int ringSizeBytes) {
        this.aeron = aeron;
        this.feedSubscription = feedSubscription;
        this.livePublication = livePublication;
        this.journalPublication = journalPublication;
        this.symbol = symbol;
        this.ring = new byte[ringSizeBytes];
        this.ringBuffer = new UnsafeBuffer(ring);
        this.durableLiveCounter =
                aeron.addCounter(CUSTOM_COUNTER_TYPE, "journal-durable-live-pos:" + symbol);
        this.durableJournalCounter =
                aeron.addCounter(CUSTOM_COUNTER_TYPE, "journal-durable-journal-pos:" + symbol);
        this.fillCounter =
                aeron.addCounter(CUSTOM_COUNTER_TYPE, "journal-bridge-fill-bytes:" + symbol);
        this.anchorQuerySubscription =
                aeron.addSubscription(OcxAeron.CHANNEL, StreamIds.bridgeAnchorQuery(symbol));
        this.anchorReplyPublication =
                aeron.addPublication(OcxAeron.CHANNEL, StreamIds.bridgeAnchorReply(symbol));
    }

    public void start() {
        thread = new Thread(this, "journal-bridge-" + symbol);
        thread.setDaemon(true);
        thread.start();
        durabilityThread = new Thread(this::durabilityLoop, "journal-durability-" + symbol);
        durabilityThread.setDaemon(true);
        durabilityThread.start();
    }

    public void close() {
        running.set(false);
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (durabilityThread != null) {
            durabilityThread.interrupt();
            try {
                durabilityThread.join(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (archive != null) {
            archive.close();
        }
        durableLiveCounter.close();
        durableJournalCounter.close();
        fillCounter.close();
    }

    /** 最新已耐久行的 live 流位置（-1 = 尚无）。 */
    public long durableLivePos() {
        return durableLivePos;
    }

    /** 最新已耐久行的 journal 流位置（-1 = 尚无）。 */
    public long durableJournalPos() {
        return durableJournalPos;
    }

    /** 环占用字节（监控/告警）。 */
    public long fillBytes() {
        return head - Math.min(liveTail, journalTail);
    }

    @Override
    public void run() {
        while (running.get()) {
            boolean worked = false;
            worked |= pollFeed();
            worked |= drainLive();
            worked |= drainJournal();
            worked |= pollAnchorQueries();
            fillCounter.set(fillBytes());
            if (!worked) {
                idle.idle();
            }
        }
    }

    /**
     * feed 消费：拷贝行进环。关键：Aeron poll 无论 handler 做什么都会推进 image 位置，
     * 因此必须按环当前空余计算 poll 预算——环满时完全不 poll（handler 提前 return = 丢数据，
     * 2026-09-01 D6 实测：录制端一卡环满，后续 feed 行被静默丢弃、live/journal 冻结）。
     */
    private boolean pollFeed() {
        long free = freeBytes();
        if (free < ROW_HDR + 1) {
            return false; // 环满：不 poll，feed 发布端自然回压
        }
        // 预算按最大行估算：环空余必须容纳全部被 poll 的行，否则 handler 提前 return
        // 会丢数据（Aeron poll 无论 handler 做什么都推进 image 位置）。
        int budget =
                (int) Math.min(FEED_POLL_BUDGET, free / (ROW_HDR + AeronWire.INBOUND_ROW_SIZE));
        final int[] worked = {0};
        feedSubscription.poll(
                (buffer, offset, length, header) -> {
                    if (length <= 0) {
                        return;
                    }
                    int need = ROW_HDR + length;
                    if (freeBytes() < need) {
                        // 预算内不应触发（环空余按最小行估算）；触发即丢数据，防御性直接放弃本轮
                        return;
                    }
                    writeIntAt(head, length);
                    writeLongAt(head + 4, 0L); // liveEnd 占位，live 消费后回写
                    writeLongAt(head + 12, 0L); // journalEnd 占位
                    int rowOff = (int) ((head + ROW_HDR) % ring.length);
                    int first = Math.min(length, ring.length - rowOff);
                    ringBuffer.putBytes(rowOff, buffer, offset, first);
                    if (first < length) {
                        ringBuffer.putBytes(0, buffer, offset + first, length - first);
                    }
                    head += ROW_HDR + length;
                    worked[0]++;
                },
                budget);
        return worked[0] > 0;
    }

    /** live 消费：行按序发布到 live 流并回写 liveEnd（只受 core 流控）。 */
    private boolean drainLive() {
        int n = 0;
        while (n < DRAIN_BUDGET && liveTail < head) {
            int len = readIntAt(liveTail);
            int rowOff = (int) ((liveTail + ROW_HDR) % ring.length);
            long result;
            if (rowOff + len <= ring.length) {
                result = livePublication.offer(ringBuffer, rowOff, len);
            } else {
                copyRowToScratch(liveTail, len);
                result = livePublication.offer(rowScratchBuffer, 0, len);
            }
            if (result < 0L) {
                if (Boolean.getBoolean("ocx.journal-bridge.debug")) {
                    System.err.println(
                            "[journal-bridge] live offer="
                                    + result
                                    + " connected="
                                    + livePublication.isConnected()
                                    + " head="
                                    + head
                                    + " liveTail="
                                    + liveTail);
                }
                break; // core 背压：下轮重试
            }
            writeLongAt(liveTail + 4, livePublication.position());
            liveTail += ROW_HDR + len;
            n++;
        }
        return n > 0;
    }

    /** journal 消费：行按序发布到 journal 流并回写 journalEnd（录制端流控由环吸收）。 */
    private boolean drainJournal() {
        int n = 0;
        while (n < DRAIN_BUDGET && journalTail < head) {
            int len = readIntAt(journalTail);
            int rowOff = (int) ((journalTail + ROW_HDR) % ring.length);
            long result;
            if (rowOff + len <= ring.length) {
                result = journalPublication.offer(ringBuffer, rowOff, len);
            } else {
                copyRowToScratch(journalTail, len);
                result = journalPublication.offer(rowScratchBuffer, 0, len);
            }
            if (result < 0L) {
                break; // 录制端背压：环吸收，下轮重试
            }
            writeLongAt(journalTail + 12, journalPublication.position());
            if (sparseHead == 0L || sparseHead % SPARSE_K == 0L) {
                int slot = (int) (sparseCount % SPARSE_CAP) * 3;
                sparseIndex[slot] = journalTail;
                sparseIndex[slot + 1] = readLongAt(journalTail + 4); // liveEnd（live 消费已回写）
                sparseIndex[slot + 2] = journalPublication.position();
                sparseCount++;
            }
            sparseHead++;
            journalTail += ROW_HDR + len;
            n++;
        }
        return n > 0;
    }

    /** 耐久轮询循环（独立线程；Archive 控制调用可能阻塞，不拖累转发主循环）。 */
    private void durabilityLoop() {
        while (running.get()) {
            try {
                pollRecorderPosition();
            } catch (Throwable t) {
                // 录制端瞬时故障不杀耐久线程：下次轮询重试
            }
            try {
                Thread.sleep(RECORDER_POLL_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    /** 录制位置轮询 + 耐久游标推进 + 计数器更新（耐久线程独占）。 */
    private void pollRecorderPosition() {
        if (archive == null) {
            ensureArchive();
        }
        if (recorderRecordingId < 0L) {
            resolveRecordingId();
        }
        if (recorderRecordingId < 0L || archive == null) {
            return;
        }
        long recorderPos = archive.getRecordingPosition(recorderRecordingId);
        if (recorderPos < 0L) {
            return;
        }
        // 从稀疏索引定位：最后一条 journalEnd ≤ recorderPos 的索引条，从其 ringOffset 前进扫描
        long idxStart = sparseScan;
        if (idxStart < 0L) {
            idxStart = 0L;
        }
        long bestIdx = -1L;
        long scan = idxStart;
        long limit = sparseCount;
        while (scan < limit) {
            int slot = (int) (scan % SPARSE_CAP) * 3;
            long jEnd = sparseIndex[slot + 2];
            if (jEnd > 0L && jEnd <= recorderPos) {
                bestIdx = scan;
            } else {
                break;
            }
            scan++;
        }
        if (bestIdx < 0L) {
            return; // recorder 尚未覆盖任何索引条：等待
        }
        sparseScan = bestIdx;
        int bestSlot = (int) (bestIdx % SPARSE_CAP) * 3;
        long cur = sparseIndex[bestSlot];
        long lastLive = -1L;
        long lastJournal = -1L;
        // 耐久边界受两侧消费游标共同约束：live 未发布的行没有 liveEnd
        long jt = journalTail;
        long lt = liveTail;
        while (cur < jt && cur < lt) {
            long jEnd = readLongAt(cur + 12);
            if (jEnd == 0L || jEnd > recorderPos) {
                break;
            }
            lastLive = readLongAt(cur + 4);
            lastJournal = jEnd;
            int len = readIntAt(cur);
            cur += ROW_HDR + len;
        }
        if (lastJournal > 0L) {
            durableLivePos = lastLive;
            durableJournalPos = lastJournal;
            durableCursor = cur;
            durableLiveCounter.set(durableLivePos);
            durableJournalCounter.set(durableJournalPos);
        }
    }

    /** 懒连接 Archive（容忍 Archive 晚于 ingress 启动/测试无 Archive）。 */
    private void ensureArchive() {
        try {
            archive =
                    AeronArchive.connect(
                            new AeronArchive.Context()
                                    .aeronDirectoryName(aeron.context().aeronDirectoryName())
                                    .controlRequestChannel("aeron:udp?endpoint=localhost:8010")
                                    .controlResponseChannel("aeron:udp?endpoint=localhost:0"));
        } catch (Exception e) {
            archive = null; // 下次轮询重试
        }
    }

    private void resolveRecordingId() {
        try {
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
                        if (sid == StreamIds.inboundJournal(symbol)
                                && recordingId > recorderRecordingId) {
                            recorderRecordingId = recordingId;
                        }
                    });
        } catch (Exception ignored) {
            // Archive 未就绪：下次轮询重试
        }
    }

    /**
     * 锚点查询轮询：core 发 [livePos]，桥回 [livePos][journalPos]（journalPosAt 精确到行）。
     * 应答发布失败静默（快照线程超时回退 durableJournalPos）。
     */
    private boolean pollAnchorQueries() {
        return anchorQuerySubscription.poll(anchorQueryHandler, 4) > 0;
    }

    private void onAnchorQuery(
            DirectBuffer buffer, int offset, int length, io.aeron.logbuffer.Header header) {
        if (length < Long.BYTES) {
            return;
        }
        long livePos = buffer.getLong(offset);
        long journalPos = journalPosAt(livePos);
        anchorScratch.putLong(0, livePos);
        anchorScratch.putLong(Long.BYTES, journalPos);
        anchorReplyPublication.offer(anchorScratch, 0, 16);
    }

    /**
     * live 位置 → journal 位置（精确到行）。稀疏索引定位 + 环内逐行前进扫描；
     * 只扫描已耐久/已发布行（liveTail/journalTail 之前），未找到返回 -1。
     */
    private long journalPosAt(long livePos) {
        long bestIdx = -1L;
        long limit = sparseCount;
        for (long i = 0L; i < limit; i++) {
            int slot = (int) (i % SPARSE_CAP) * 3;
            long le = sparseIndex[slot + 1];
            if (le > 0L && le <= livePos) {
                bestIdx = i;
            } else if (le > 0L) {
                break; // 稀疏条目按 liveEnd 单调（同环序），超出即停
            }
        }
        if (bestIdx < 0L) {
            return -1L;
        }
        int slot = (int) (bestIdx % SPARSE_CAP) * 3;
        long cur = sparseIndex[slot];
        long lastJournal = sparseIndex[slot + 2];
        long jt = journalTail;
        long lt = liveTail;
        while (cur < jt && cur < lt) {
            long le = readLongAt(cur + 4);
            if (le == 0L || le > livePos) {
                break;
            }
            lastJournal = readLongAt(cur + 12);
            int len = readIntAt(cur);
            cur += ROW_HDR + len;
        }
        return lastJournal;
    }

    private long freeBytes() {
        return ring.length - (head - Math.min(liveTail, journalTail));
    }

    /** 跨环边界的行拷贝到 scratch（Aeron offer 需要连续字节）。 */
    private void copyRowToScratch(long tail, int len) {
        int off = (int) ((tail + ROW_HDR) % ring.length);
        int first = Math.min(len, ring.length - off);
        rowScratchBuffer.putBytes(0, ringBuffer, off, first);
        if (first < len) {
            rowScratchBuffer.putBytes(first, ringBuffer, 0, len - first);
        }
    }

    private void writeIntAt(long ringOffset, int value) {
        int off = (int) (ringOffset % ring.length);
        if (off + 4 <= ring.length) {
            ring[off] = (byte) (value >>> 24);
            ring[off + 1] = (byte) (value >>> 16);
            ring[off + 2] = (byte) (value >>> 8);
            ring[off + 3] = (byte) value;
        } else {
            int first = ring.length - off;
            for (int i = 0; i < first; i++) {
                ring[off + i] = (byte) (value >>> (24 - i * 8));
            }
            for (int i = first; i < 4; i++) {
                ring[i - first] = (byte) (value >>> (24 - i * 8));
            }
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

    private void writeLongAt(long ringOffset, long value) {
        int off = (int) (ringOffset % ring.length);
        for (int i = 0; i < 8; i++) {
            ring[(off + i) % ring.length] = (byte) (value >>> (56 - i * 8));
        }
    }

    private long readLongAt(long ringOffset) {
        int off = (int) (ringOffset % ring.length);
        long v = 0L;
        for (int i = 0; i < 8; i++) {
            v = (v << 8) | (ring[(off + i) % ring.length] & 0xFF);
        }
        return v;
    }
}
