package com.ocx.egress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.ocx.aeron.AeronWire;
import com.ocx.aeron.OcxAeron;
import com.ocx.aeron.StreamIds;

import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 主备热备技术验证（2026-09-01）：Aeron Archive replication 能否把"主机"入站
 * recording 实时复制到"备机" Archive，以及复制滞后（RTO 上限的关键输入）。
 *
 * <p>拓扑：主 Archive（源）记录入站流 → 备 Archive（目标）replicate 该 recording
 * （stopPosition=NULL 实时追）。校验：目标 recording 与源 recording 的
 * startPosition/stopPosition/字节数完全一致；复制追平耗时即滞后。</p>
 */
class ArchiveReplicationTest {

    private static final String SYMBOL = "BTCUSDT";
    private static final int FRAMES = 200_000;
    private static final String SRC_CONTROL = "aeron:udp?endpoint=localhost:28010";
    private static final String SRC_REPLICATION = "aeron:udp?endpoint=localhost:28011";
    private static final String DST_CONTROL = "aeron:udp?endpoint=localhost:28020";
    private static final String DST_REPLICATION = "aeron:udp?endpoint=localhost:28021";
    private static final int REPLAY_STREAM_ID = 590_001;

    @Test
    void replicatesInboundRecordingAndCatchesUp() throws Exception {
        try (OcxAeron srcAeron = OcxAeron.embedded();
                OcxAeron dstAeron = OcxAeron.embedded();
                Archive srcArchive =
                        launchArchive(
                                srcAeron.aeronDirectoryName(), SRC_CONTROL, SRC_REPLICATION, null);
                Archive dstArchive =
                        launchArchive(
                                dstAeron.aeronDirectoryName(),
                                DST_CONTROL,
                                DST_REPLICATION,
                                "aeron:udp?endpoint=localhost:0")) {

            try (AeronArchive srcClient = connect(srcAeron.aeronDirectoryName(), SRC_CONTROL);
                    AeronArchive dstClient = connect(dstAeron.aeronDirectoryName(), DST_CONTROL);
                    Publication pub = srcAeron.addInboundPublication(SYMBOL)) {

                int streamId = StreamIds.inbound(SYMBOL);
                srcClient.startRecording(OcxAeron.CHANNEL, streamId, SourceLocation.LOCAL);
                long srcRecordingId = awaitInboundRecording(srcClient, streamId);

                // 发布全部输入帧（264B 定长行，与生产入站行同尺寸）
                publishFrames(pub, FRAMES);
                // 停录源 recording，stopPosition 落定
                srcClient.stopRecording(OcxAeron.CHANNEL, streamId);
                long srcStop = awaitRecordingStopped(srcClient, srcRecordingId);

                // 备机启动复制：stopPosition=NULL → 实时追到录制结束
                long replicationId =
                        dstClient.replicate(
                                srcRecordingId,
                                AeronArchive.NULL_POSITION,
                                srcClient.context().controlRequestStreamId(),
                                srcClient.context().controlRequestChannel(),
                                "");
                assertNotEquals(
                        AeronArchive.NULL_POSITION, replicationId, "replicate 必须返回有效 session");

                // 等备机复制 recording 出现并追平
                long dstRecordingId = awaitReplicatedRecording(dstClient, streamId);
                long t0 = System.nanoTime();
                long dstStop = awaitReplicationCaughtUp(dstClient, dstRecordingId, srcStop);
                long lagMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);

                // 校验：源/备 recording 起止位置与字节数完全一致
                RecordingMeta srcMeta = recordingMeta(srcClient, srcRecordingId);
                RecordingMeta dstMeta = recordingMeta(dstClient, dstRecordingId);
                assertEquals(srcMeta.startPosition, dstMeta.startPosition, "startPosition 必须一致");
                assertEquals(srcStop, srcMeta.stopPosition);
                assertEquals(srcStop, dstStop, "stopPosition 必须一致");
                assertEquals(
                        srcMeta.stopPosition - srcMeta.startPosition,
                        dstMeta.stopPosition - dstMeta.startPosition,
                        "复制字节数必须一致");

                System.out.printf(
                        "[ArchiveReplicationTest] frames=%d srcStop=%d dstStop=%d "
                                + "catchUpMs=%d%n",
                        FRAMES, srcStop, dstStop, lagMs);
                assertTrue(lagMs < 30_000, "复制追平耗时异常: " + lagMs + "ms");
            }
        }
    }

    /**
     * 备机陪跑的完整输入链：replicate 持续追活跃 recording（源不停止）→
     * 备机对复制 recording 做 live replay（从 0 起，跟随增长）→ 订阅者收到全部帧。
     * 即"备机 Archive 复制 + 备机 bridge 回放喂备机 core 入站环"的机制验证。
     */
    @Test
    void replicatesActiveRecordingAndLiveReplays() throws Exception {
        try (OcxAeron srcAeron = OcxAeron.embedded();
                OcxAeron dstAeron = OcxAeron.embedded();
                Archive srcArchive =
                        launchArchive(
                                srcAeron.aeronDirectoryName(), SRC_CONTROL, SRC_REPLICATION, null);
                Archive dstArchive =
                        launchArchive(
                                dstAeron.aeronDirectoryName(),
                                DST_CONTROL,
                                DST_REPLICATION,
                                "aeron:udp?endpoint=localhost:0")) {

            try (AeronArchive srcClient = connect(srcAeron.aeronDirectoryName(), SRC_CONTROL);
                    AeronArchive dstClient = connect(dstAeron.aeronDirectoryName(), DST_CONTROL);
                    Publication pub = srcAeron.addInboundPublication(SYMBOL)) {

                int streamId = StreamIds.inbound(SYMBOL);
                srcClient.startRecording(OcxAeron.CHANNEL, streamId, SourceLocation.LOCAL);
                long srcRecordingId = awaitInboundRecording(srcClient, streamId);

                // 源 recording 持续活跃：复制与发布并行
                Thread publisher = new Thread(() -> publishFrames(pub, FRAMES));
                publisher.start();

                // 备机复制（NULL_POSITION = 实时追到 recording 结束）
                long replicationId =
                        dstClient.replicate(
                                srcRecordingId,
                                AeronArchive.NULL_POSITION,
                                srcClient.context().controlRequestStreamId(),
                                srcClient.context().controlRequestChannel(),
                                "");
                assertNotEquals(
                        AeronArchive.NULL_POSITION, replicationId, "replicate 必须返回有效 session");
                long dstRecordingId = awaitReplicatedRecording(dstClient, streamId);
                awaitReplicationStarted(dstClient, dstRecordingId);

                // 备机 live replay（从 recording 起点，跟随复制增长；replay 数据回本地媒体驱动）
                Subscription replaySub =
                        dstClient.replay(
                                dstRecordingId,
                                0L,
                                AeronArchive.NULL_POSITION,
                                OcxAeron.CHANNEL,
                                REPLAY_STREAM_ID);
                assertNotNull(replaySub, "replay 必须返回有效 subscription");

                AtomicInteger received = new AtomicInteger();
                AtomicInteger errors = new AtomicInteger();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (System.nanoTime() < deadline && received.get() < FRAMES) {
                    replaySub.poll(
                            (buffer, offset, length, header) -> {
                                if (length == AeronWire.INBOUND_ROW_SIZE) {
                                    received.incrementAndGet();
                                } else {
                                    errors.incrementAndGet();
                                }
                            },
                            256);
                    Thread.sleep(2);
                }
                publisher.join(TimeUnit.SECONDS.toMillis(10));

                System.out.printf(
                        "[ArchiveReplicationTest] liveReplay received=%d errors=%d%n",
                        received.get(), errors.get());
                assertEquals(FRAMES, received.get(), "live replay 应收齐全部帧（复制追平 + 持续增长）");
                assertEquals(0, errors.get(), "live replay 不应有非 264B 行");
            }
        }
    }

    private static Archive launchArchive(
            String aeronDir,
            String controlChannel,
            String replicationChannel,
            String clientResponseChannel) {
        Archive.Context ctx =
                new Archive.Context()
                        .aeronDirectoryName(aeronDir)
                        .archiveDir(new File(new File(aeronDir), "archive"))
                        .controlChannel(controlChannel)
                        .replicationChannel(replicationChannel)
                        .recordingEventsEnabled(false);
        if (clientResponseChannel != null) {
            // 目标 Archive 内部复制 client：control response 通道（源 Archive 响应送达处）
            ctx.archiveClientContext(
                    new AeronArchive.Context().controlResponseChannel(clientResponseChannel));
        }
        return Archive.launch(ctx);
    }

    private static AeronArchive connect(String aeronDir, String controlChannel) {
        return AeronArchive.connect(
                new AeronArchive.Context()
                        .aeronDirectoryName(aeronDir)
                        .controlRequestChannel(controlChannel)
                        .controlResponseChannel("aeron:udp?endpoint=localhost:0"));
    }

    private static void publishFrames(Publication pub, int count) {
        UnsafeBuffer row = new UnsafeBuffer(new byte[AeronWire.INBOUND_ROW_SIZE]);
        for (int i = 0; i < count; i++) {
            row.setMemory(0, AeronWire.INBOUND_ROW_SIZE, (byte) (i & 0xFF));
            long result;
            while ((result = pub.offer(row, 0, AeronWire.INBOUND_ROW_SIZE)) < 0L) {
                Thread.onSpinWait();
            }
        }
    }

    private static long awaitInboundRecording(AeronArchive client, int streamId)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            long[] found = {-1L};
            client.listRecordings(
                    0L,
                    Integer.MAX_VALUE,
                    (cs,
                            corr,
                            recordingId,
                            startTs,
                            triggerTs,
                            startPos,
                            stopPos,
                            it,
                            sfl,
                            tbl,
                            mtu,
                            sid,
                            streamId2,
                            sc,
                            oc,
                            si) -> {
                        if (streamId2 == streamId) {
                            found[0] = recordingId;
                        }
                    });
            if (found[0] >= 0L) {
                return found[0];
            }
            Thread.sleep(20);
        }
        fail("入站 recording 未建立: stream=" + streamId);
        return -1L;
    }

    /** 备机复制出的 recording：同样按 streamId 在目标 Archive 目录中检索。 */
    private static long awaitReplicatedRecording(AeronArchive client, int streamId)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            long[] found = {-1L};
            client.listRecordings(
                    0L,
                    Integer.MAX_VALUE,
                    (cs,
                            corr,
                            recordingId,
                            startTs,
                            triggerTs,
                            startPos,
                            stopPos,
                            it,
                            sfl,
                            tbl,
                            mtu,
                            sid,
                            streamId2,
                            sc,
                            oc,
                            si) -> {
                        if (streamId2 == streamId) {
                            found[0] = Math.max(found[0], recordingId);
                        }
                    });
            if (found[0] >= 0L) {
                return found[0];
            }
            Thread.sleep(20);
        }
        fail("备机复制 recording 未出现: stream=" + streamId);
        return -1L;
    }

    /** 等待复制 recording 开始写入（getRecordingPosition > 0），replay 起点才有效。 */
    private static void awaitReplicationStarted(AeronArchive client, long recordingId)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            try {
                long pos = client.getRecordingPosition(recordingId);
                if (pos != AeronArchive.NULL_POSITION && pos > 0L) {
                    return;
                }
            } catch (io.aeron.archive.client.ArchiveException ignored) {
                // recorder 尚未附着/复制未启动
            }
            Thread.sleep(5);
        }
        fail("复制 recording 未开始写入: id=" + recordingId);
    }

    /** 等待 recording 停止（stopPosition 落定）并返回 stopPosition。 */
    private static long awaitRecordingStopped(AeronArchive client, long recordingId)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            RecordingMeta meta = recordingMeta(client, recordingId);
            if (meta.stopPosition != AeronArchive.NULL_POSITION) {
                return meta.stopPosition;
            }
            Thread.sleep(20);
        }
        fail("recording 未停止: id=" + recordingId);
        return -1L;
    }

    /**
     * 等待复制 recording 追平源 recording（源已停止）：复制中 stopPosition 可能为 0/未定，
     * 以实时位置 getRecordingPosition 追上 srcStop 为准，追平后 stopPosition 落定。
     */
    private static long awaitReplicationCaughtUp(
            AeronArchive client, long recordingId, long srcStop) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            long live = client.getRecordingPosition(recordingId);
            RecordingMeta meta = recordingMeta(client, recordingId);
            System.out.printf(
                    "[ArchiveReplicationTest] catchup live=%d stop=%d srcStop=%d%n",
                    live, meta.stopPosition, srcStop);
            if (meta.stopPosition != AeronArchive.NULL_POSITION && meta.stopPosition != 0L) {
                return meta.stopPosition;
            }
            if (live != AeronArchive.NULL_POSITION && live >= srcStop) {
                Thread.sleep(50);
                meta = recordingMeta(client, recordingId);
                if (meta.stopPosition != AeronArchive.NULL_POSITION && meta.stopPosition != 0L) {
                    return meta.stopPosition;
                }
            }
            Thread.sleep(50);
        }
        fail("复制未追平: id=" + recordingId + " srcStop=" + srcStop);
        return -1L;
    }

    private static RecordingMeta recordingMeta(AeronArchive client, long recordingId) {
        AtomicLong startPos = new AtomicLong(-1L);
        AtomicLong stopPos = new AtomicLong(-1L);
        client.listRecording(
                recordingId,
                (cs,
                        corr,
                        rid,
                        startTs,
                        triggerTs,
                        startPos1,
                        stopPos1,
                        it,
                        sfl,
                        tbl,
                        mtu,
                        sid,
                        streamId,
                        sc,
                        oc,
                        si) -> {
                    startPos.set(startPos1);
                    stopPos.set(stopPos1);
                });
        return new RecordingMeta(startPos.get(), stopPos.get());
    }

    private record RecordingMeta(long startPosition, long stopPosition) {}
}
