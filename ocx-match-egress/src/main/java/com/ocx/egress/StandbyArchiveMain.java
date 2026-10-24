package com.ocx.egress;

import com.ocx.aeron.ControlStream;
import com.ocx.aeron.OcxAeron;
import com.ocx.aeron.StreamIds;
import com.ocx.config.ConfigLoader;
import com.ocx.config.MatchEngineConfig;

import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;

import java.io.File;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * 备机 Archive（主备热备，2026-09-01）：启动本机 Archive 并对**主机**的入站
 * recording 发起持续复制（stopPosition=NULL 实时追）；同时**常驻录制备机入站流**
 * （{@link StreamIds#standbyInbound(String)}，bridge 转发 + 备机 ingress 接管后的完整
 * 输入 journal——2026-09-01 修复"接管后新输入无日志"缺口）以及备机自身的快照流
 * （影子盘：备机 core 快照照常落盘，供接管后恢复）。
 *
 * <p>备机陪跑的输入链（与 ReplicationIngressBridge 配合）：
 * 主机 Archive 入站 journal recording（bridge 模式为 inboundJournal，兼容模式为
 * inbound）→ 本进程 replicate 复制到备机 Archive → bridge 对复制 recording live
 * replay → 备机 journal bridge → 备机 core 入站环 + 备机 journal 录制。主机链路
 * 零改动；2026-09-02 接入桥模式：录制/复制目标统一走 effective journal 流。</p>
 *
 * <p>参数：{@code -Docx.aeron.dir}（备机媒体驱动目录）、
 * {@code -Docx.archive.dir}（备机 Archive 目录）、
 * {@code -Docx.standby.archive.control=}（备机本机控制通道，默认 localhost:18010）、
 * {@code -Docx.standby.archive.replication=}（备机复制通道，默认 localhost:18011）、
 * {@code -Docx.standby.primary.control=}（主机 Archive 控制通道，如
 * {@code aeron:udp?endpoint=192.168.0.120:8010}）、
 * {@code -Docx.standby.symbols=}（逗号分隔，缺省读引擎配置）。</p>
 *
 * <p>接管输出（2026-09-01 阶段 A）：备机 egress 全程运行写本机 Archive（影子输出，
 * 下游接管前不消费），因此本进程除复制入站外还常驻录制备机自身的成交/深度/DLT 流
 * （方案 A 常驻 recording，规避运行中 startRecording 的 image 回调缺陷）。
 * 开关 {@code -Docx.standby.record-outbound=}（默认 true）。</p>
 */
public final class StandbyArchiveMain {

    private static final CountDownLatch LATCH = new CountDownLatch(1);

    private StandbyArchiveMain() {}

    public static void main(String[] args) throws InterruptedException {
        String aeronDir = System.getProperty("ocx.aeron.dir", "/tmp/ocx-aeron");
        String archiveDir = System.getProperty("ocx.archive.dir", aeronDir + "/archive");
        String localControl =
                System.getProperty(
                        "ocx.standby.archive.control", "aeron:udp?endpoint=localhost:18010");
        String localReplication =
                System.getProperty(
                        "ocx.standby.archive.replication", "aeron:udp?endpoint=localhost:18011");
        String primaryControl = System.getProperty("ocx.standby.primary.control");
        if (primaryControl == null || primaryControl.isBlank()) {
            throw new IllegalArgumentException(
                    "备机必须配置 -Docx.standby.primary.control=（主机 Archive 控制通道）");
        }
        int fileSyncLevel = Integer.getInteger("ocx.archive.file.sync.level", 0);
        boolean recordOutbound =
                Boolean.parseBoolean(System.getProperty("ocx.standby.record-outbound", "true"));
        Set<String> symbols = resolveSymbols(args);

        Archive archive =
                Archive.launch(
                        new Archive.Context()
                                .aeronDirectoryName(aeronDir)
                                .archiveDir(new File(archiveDir))
                                .controlChannel(localControl)
                                .replicationChannel(localReplication)
                                .recordingEventsEnabled(false)
                                .fileSyncLevel(fileSyncLevel)
                                .catalogFileSyncLevel(fileSyncLevel)
                                // 备机 Archive 内部复制 client：control response 通道（主机响应送达处）
                                .archiveClientContext(
                                        new AeronArchive.Context()
                                                .controlResponseChannel(
                                                        "aeron:udp?endpoint=localhost:0")));

        try (AeronArchive localClient =
                        AeronArchive.connect(
                                new AeronArchive.Context()
                                        .aeronDirectoryName(aeronDir)
                                        .controlRequestChannel(localControl)
                                        .controlResponseChannel("aeron:udp?endpoint=localhost:0"));
                AeronArchive primaryClient =
                        AeronArchive.connect(
                                new AeronArchive.Context()
                                        .aeronDirectoryName(aeronDir)
                                        .controlRequestChannel(primaryControl)
                                        .controlResponseChannel(
                                                "aeron:udp?endpoint=localhost:0"))) {

            for (String symbol : symbols) {
                int streamId = StreamIds.inboundJournalEffective(symbol);
                long primaryRecordingId = findLatestRecording(primaryClient, streamId);
                if (primaryRecordingId < 0L) {
                    throw new IllegalStateException(
                            "主机尚无入站 recording: stream=" + streamId + "（需先启动主机 Archive 并开始录制）");
                }
                long replicationId =
                        localClient.replicate(
                                primaryRecordingId,
                                AeronArchive.NULL_POSITION,
                                primaryClient.context().controlRequestStreamId(),
                                primaryClient.context().controlRequestChannel(),
                                "");
                if (replicationId == AeronArchive.NULL_POSITION) {
                    throw new IllegalStateException("replicate 失败: stream=" + streamId);
                }
                System.err.println(
                        "[StandbyArchiveMain] replicate started symbol="
                                + symbol
                                + " srcRecording="
                                + primaryRecordingId
                                + " replication="
                                + replicationId);

                // 备机影子快照录制（备机 core 快照照常落盘，接管后恢复可用）
                localClient.startRecording(
                        OcxAeron.CHANNEL, StreamIds.snapshot(symbol), SourceLocation.LOCAL);
                // 备机完整输入 journal（2026-09-01）：bridge 转发（复制输入）与
                // 备机 ingress（接管后新输入）都经备机 journal bridge 发布，常驻录制
                // （bridge 模式录 inboundJournal，兼容模式录 standbyInbound）
                localClient.startRecording(
                        OcxAeron.CHANNEL,
                        StreamIds.standbyJournalEffective(symbol),
                        SourceLocation.LOCAL);
                if (recordOutbound) {
                    // 备机影子成交/深度录制：备机 egress 全程发布，接管前下游不消费
                    localClient.startRecording(
                            OcxAeron.CHANNEL, StreamIds.archiveTrade(symbol), SourceLocation.LOCAL);
                    localClient.startRecording(
                            OcxAeron.CHANNEL, StreamIds.archiveDepth(symbol), SourceLocation.LOCAL);
                }
            }
            if (recordOutbound) {
                localClient.startRecording(
                        OcxAeron.CHANNEL, StreamIds.ARCHIVE_DLT_STREAM, SourceLocation.LOCAL);
            }
        }

        System.err.println(
                "[StandbyArchiveMain] STANDBY_ARCHIVE_READY dir="
                        + archiveDir
                        + " aeron="
                        + aeronDir
                        + " symbols="
                        + symbols
                        + " primaryControl="
                        + primaryControl
                        + " recordOutbound="
                        + recordOutbound);
        ControlStream.publishReady(aeronDir, "standby-archive");
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    archive.close();
                                    LATCH.countDown();
                                },
                                "standby-archive-shutdown-hook"));
        LATCH.await();
    }

    /** 主机入站流上最新一条 recording（M1 取最大 recordingId；轮转/多段为后续工作）。 */
    private static long findLatestRecording(AeronArchive client, int streamId) {
        long[] latest = {-1L};
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
                    if (streamId2 == streamId && recordingId > latest[0]) {
                        latest[0] = recordingId;
                    }
                });
        return latest[0];
    }

    private static Set<String> resolveSymbols(String[] args) {
        String prop = System.getProperty("ocx.standby.symbols");
        if (prop != null && !prop.isBlank()) {
            Set<String> set = new LinkedHashSet<>();
            for (String s : prop.split(",")) {
                if (!s.isBlank()) {
                    set.add(s.trim());
                }
            }
            if (!set.isEmpty()) {
                return set;
            }
        }
        try {
            MatchEngineConfig cfg = ConfigLoader.load(args);
            return new LinkedHashSet<>(cfg.getMatchEngineProperties().getSymbols().keySet());
        } catch (Exception e) {
            return Set.of("BTCUSDT");
        }
    }
}
