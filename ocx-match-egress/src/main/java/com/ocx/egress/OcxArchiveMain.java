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
 * 独立 Archive 进程（持久化基础设施）：连接共享媒体驱动目录，记录入站 journal 与
 * 出站持久化流——成交/深度/DLT。
 *
 * <p>恢复主路径 = 入站 recording（{@link StreamIds#inbound(String)}）持续记录，
 * 崩溃后 core 用 {@code AeronArchive.replay} 回放入站帧、重跑撮合重建簿状态
 * （见 RecoveryService）。记录订阅在发布之前注册，数据不丢；发布方感知不到
 * Archive 存在，只对共享内存环 offer。媒体驱动目录 {@code -Docx.aeron.dir=}
 * （默认 /tmp/ocx-aeron），记录目录 {@code -Docx.archive.dir=}（默认
 * {@code aeronDir/archive}），交易对 {@code -Docx.archive.symbols=}（缺省读引擎配置），
 * 文件同步级别 {@code -Docx.archive.file.sync.level=}（0=NO_SYNC/1=SYNC/2=FSYNC，默认 0）。</p>
 */
public final class OcxArchiveMain {

    private static final CountDownLatch LATCH = new CountDownLatch(1);

    private OcxArchiveMain() {}

    public static void main(String[] args) throws InterruptedException {
        String aeronDir = System.getProperty("ocx.aeron.dir", "/tmp/ocx-aeron");
        String archiveDir = System.getProperty("ocx.archive.dir", aeronDir + "/archive");
        int fileSyncLevel = Integer.getInteger("ocx.archive.file.sync.level", 0);
        // 入站 journal 录制开关（对照实验/降持久性形态用；默认 true = 输入 journal 恢复）
        boolean recordInbound =
                Boolean.parseBoolean(System.getProperty("ocx.archive.record-inbound", "true"));
        // 出站 recording 开关（成交/深度/DLT；默认 true = 出站持久化日志）。
        // 与 record-inbound 配合可构造"零落盘"纯计算形态：两开关全关 + 快照
        // interval-ms=0 时 Archive 完全空转，用于探顶撮合/内存上限（2026-08-31）。
        boolean recordOutbound =
                Boolean.parseBoolean(System.getProperty("ocx.archive.record-outbound", "true"));
        Set<String> symbols = resolveSymbols(args);

        Archive archive =
                Archive.launch(
                        new Archive.Context()
                                .aeronDirectoryName(aeronDir)
                                .archiveDir(new File(archiveDir))
                                .controlChannel("aeron:udp?endpoint=localhost:8010")
                                .replicationChannel("aeron:udp?endpoint=localhost:8011")
                                .recordingEventsEnabled(false)
                                // 错误可见性（2026-09-02）：Archive 内部错误默认只进计数
                                // （Archive Errors counter，消息不可读）；打印到 stderr 便于
                                // 定位录制会话静默终止（D10-D15 ~281s 全会话停止根因排查）。
                                .errorHandler(t -> System.err.println("[OcxArchive] ERROR: " + t))
                                .fileSyncLevel(fileSyncLevel)
                                .catalogFileSyncLevel(fileSyncLevel));

        try (AeronArchive client =
                AeronArchive.connect(
                        new AeronArchive.Context()
                                .aeronDirectoryName(aeronDir)
                                .controlRequestChannel("aeron:udp?endpoint=localhost:8010")
                                .controlResponseChannel("aeron:udp?endpoint=localhost:0"))) {
            for (String symbol : symbols) {
                // 入站流 recording（2026-08-29 输入 journal）：恢复 = 快照 + 回放入站重建簿
                if (recordInbound) {
                    // 2026-09-01 录制解耦：bridge 模式录 journal 流，兼容模式录 live 流
                    client.startRecording(
                            OcxAeron.CHANNEL,
                            StreamIds.inboundJournalEffective(symbol),
                            SourceLocation.LOCAL);
                }
                if (recordOutbound) {
                    client.startRecording(
                            OcxAeron.CHANNEL, StreamIds.archiveTrade(symbol), SourceLocation.LOCAL);
                    client.startRecording(
                            OcxAeron.CHANNEL, StreamIds.archiveDepth(symbol), SourceLocation.LOCAL);
                }
                // 快照流常驻 recording（2026-08-31 方案 A）：快照线程每 10s 往流里写
                // BEGIN(seq)+META+ORDER+STOP+END 分段；不再每张 start/stop recording——
                // 运行中临时 startRecording 会踩 Aeron Archive available-image 回调
                // 时序问题（recording 不创建）；常驻录制与入站/成交同构，稳定工作。
                client.startRecording(
                        OcxAeron.CHANNEL, StreamIds.snapshot(symbol), SourceLocation.LOCAL);
            }
            if (recordOutbound) {
                client.startRecording(
                        OcxAeron.CHANNEL, StreamIds.ARCHIVE_DLT_STREAM, SourceLocation.LOCAL);
            }
        }

        System.err.println(
                "[OcxArchiveMain] ARCHIVE_READY dir="
                        + archiveDir
                        + " aeron="
                        + aeronDir
                        + " symbols="
                        + symbols
                        + " fileSyncLevel="
                        + fileSyncLevel
                        + " recordInbound="
                        + recordInbound
                        + " recordOutbound="
                        + recordOutbound);
        ControlStream.publishReady(aeronDir, "archive");
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    archive.close();
                                    LATCH.countDown();
                                },
                                "archive-shutdown-hook"));
        LATCH.await();
    }

    private static Set<String> resolveSymbols(String[] args) {
        String prop = System.getProperty("ocx.archive.symbols");
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
