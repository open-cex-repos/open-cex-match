package com.ocx.aeron;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;

import org.agrona.concurrent.status.CountersReader;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;

/**
 * 录制滞后探针（2026-09-01，录制端解耦专项）：每 200ms 采样关键流的
 * 发布端位置（媒体驱动 pub-pos 计数器）与 Archive 录制位置
 * （{@code AeronArchive.getRecordingPosition}），输出 TSV：
 * {@code epochMs streamId pubPos recPos lagBytes}。
 *
 * <p>用途：定位全形态锯齿摆动的背压点——若某流 lagBytes 在 TPS 低谷同步
 * 放大，则录制端是该流的节流阀；配合 perf-flatline-analyze.py 做滞后相关。</p>
 *
 * <p>用法：{@code java -cp ocx-match-ingress-1.0.jar -Docx.aeron.dir=...
 * -Docx.probe.streams=29916,227916,307916 -Docx.probe.out=lag.tsv
 * -Docx.probe.interval-ms=200 com.ocx.aeron.RecorderLagProbe}</p>
 */
public final class RecorderLagProbe {

    /** 发布端位置计数器：1.52.2 的 type 因流而异（49/110/120/151…），统一按 label 前缀+后缀匹配。 */
    private static final String PUB_POS_LABEL_PREFIX = "pub-pos";

    private static final long POLL_INTERVAL_MS = Long.getLong("ocx.probe.interval-ms", 200L);

    private RecorderLagProbe() {}

    public static void main(String[] args) throws Exception {
        String aeronDir = System.getProperty("ocx.aeron.dir", "/tmp/ocx-aeron");
        String out = System.getProperty("ocx.probe.out", "/tmp/recorder-lag.tsv");
        String[] streamSpecs =
                System.getProperty("ocx.probe.streams", "29916,227916,307916").split(",");
        int[] streams = new int[streamSpecs.length];
        for (int i = 0; i < streamSpecs.length; i++) {
            streams[i] = Integer.parseInt(streamSpecs[i].trim());
        }

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
                AeronArchive archive =
                        AeronArchive.connect(
                                new AeronArchive.Context()
                                        .aeronDirectoryName(aeronDir)
                                        .controlRequestChannel("aeron:udp?endpoint=localhost:8010")
                                        .controlResponseChannel("aeron:udp?endpoint=localhost:0"));
                PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(out)))) {

            CountersReader counters = aeron.countersReader();
            if (Boolean.getBoolean("ocx.probe.debug-counters")) {
                counters.forEach(
                        (id, typeId, label) -> {
                            if (label.startsWith(PUB_POS_LABEL_PREFIX)) {
                                System.err.println(
                                        "COUNTER type="
                                                + typeId
                                                + " id="
                                                + id
                                                + " label=["
                                                + label
                                                + "]");
                            }
                        });
            }
            long[] recordingIds = new long[streams.length];
            for (int i = 0; i < recordingIds.length; i++) {
                recordingIds[i] = -1L;
            }
            pw.println("epochMs streamId pubPos recPos lagBytes");
            long start = System.currentTimeMillis();
            while (true) {
                // 首次与重试：按 streamId 解析 recordingId（常驻 recording 不轮转）
                boolean anyMissing = false;
                for (int i = 0; i < streams.length; i++) {
                    if (recordingIds[i] < 0L) {
                        recordingIds[i] = findRecording(archive, streams[i]);
                        anyMissing |= recordingIds[i] < 0L;
                    }
                }
                for (int i = 0; i < streams.length; i++) {
                    long pub = publicationPosition(counters, streams[i]);
                    long rec =
                            recordingIds[i] < 0L
                                    ? -1L
                                    : archive.getRecordingPosition(recordingIds[i]);
                    pw.printf(
                            "%d %d %d %d %d%n",
                            System.currentTimeMillis(),
                            streams[i],
                            pub,
                            rec,
                            pub > 0 && rec > 0 ? pub - rec : -1L);
                }
                pw.flush();
                if (Thread.interrupted()) {
                    break;
                }
                Thread.sleep(POLL_INTERVAL_MS);
            }
        }
    }

    private static long findRecording(AeronArchive archive, int streamId) {
        final long[] found = {-1L};
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
                    if (sid == streamId && recordingId > found[0]) {
                        found[0] = recordingId;
                    }
                });
        return found[0];
    }

    private static long publicationPosition(CountersReader counters, int streamId) {
        final long[] found = {0L};
        counters.forEach(
                (counterId, typeId, labelBuffer, label) -> {
                    if (label.startsWith(PUB_POS_LABEL_PREFIX)
                            && label.endsWith(streamId + " " + OcxAeron.CHANNEL)) {
                        found[0] = Math.max(found[0], counters.getCounterValue(counterId));
                    }
                });
        return found[0];
    }
}
