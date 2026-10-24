package com.ocx.egress;

import com.ocx.aeron.OcxAeron;
import com.ocx.aeron.StreamIds;
import com.ocx.codec.IoCodec;

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Archive 探测工具：列出全部 recording（stream/字节数/位置），可选回放统计成交帧数
 * （{@code -Docx.probe.count-trades=true}，默认 symbol 取 {@code -Docx.probe.symbol=}）。
 * 压测脚本用它在结束后做落盘验证；未来 match 域之外的冷读端可复用同一套回放 API。
 */
public final class ArchiveProbe {

    /** 回放目的地流（避开 archive 记录流 200k-300k 与系统流）。 */
    private static final int REPLAY_STREAM_BASE = 400_000;

    private static final long REPLAY_QUIET_MS = 3_000L;
    private static final long REPLAY_DEADLINE_MS = 10 * 60_000L;

    public record Recording(
            long recordingId,
            long startPosition,
            long stopPosition,
            long startTimestamp,
            long triggerTimestamp,
            int streamId,
            String channel,
            String sourceIdentity) {}

    private ArchiveProbe() {}

    public static void main(String[] args) {
        String aeronDir = System.getProperty("ocx.aeron.dir", "/tmp/ocx-aeron");
        String symbol = System.getProperty("ocx.probe.symbol", "BTCUSDT");
        boolean countTrades = Boolean.getBoolean("ocx.probe.count-trades");
        // 对账/冷读可指向任一 Archive（主 8010 / 备 18010；默认 8010 兼容旧调用）
        String archiveControl =
                System.getProperty(
                        "ocx.archive.control-channel", "aeron:udp?endpoint=localhost:8010");

        List<Recording> recordings = new ArrayList<>();
        try (AeronArchive client =
                AeronArchive.connect(
                        new AeronArchive.Context()
                                .aeronDirectoryName(aeronDir)
                                .controlRequestChannel(archiveControl)
                                .controlResponseChannel("aeron:udp?endpoint=localhost:0"))) {
            client.listRecordings(
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
                            streamId,
                            strippedChannel,
                            originalChannel,
                            sourceIdentity) ->
                            recordings.add(
                                    new Recording(
                                            recordingId,
                                            startPosition,
                                            stopPosition,
                                            startTimestamp,
                                            triggerTimestamp,
                                            streamId,
                                            strippedChannel,
                                            sourceIdentity)));

            System.out.println("=== Archive recordings (" + recordings.size() + ") ===");
            for (Recording r : recordings) {
                long stop = resolvedStop(client, r);
                long bytes = stop != AeronArchive.NULL_POSITION ? stop - r.startPosition() : 0L;
                System.out.println(
                        "stream="
                                + r.streamId()
                                + " recordingId="
                                + r.recordingId()
                                + " start="
                                + r.startPosition()
                                + " stop="
                                + r.stopPosition()
                                + " resolvedStop="
                                + stop
                                + " bytes="
                                + bytes
                                + " channel="
                                + r.channel()
                                + " src="
                                + r.sourceIdentity());
            }

            if (countTrades) {
                long tradeStream = StreamIds.archiveTrade(symbol);
                Recording trade =
                        recordings.stream()
                                .filter(
                                        r ->
                                                r.streamId() == tradeStream
                                                        && resolvedStop(client, r)
                                                                > r.startPosition())
                                .findFirst()
                                .orElse(null);
                if (trade == null) {
                    System.out.println(
                            "trade recording not found for symbol="
                                    + symbol
                                    + " stream="
                                    + tradeStream);
                } else {
                    System.out.println(
                            "trade " + symbol + " frames=" + countTradeFrames(client, trade));
                }
            }
        }
    }

    /** recording 结束前 stopPosition 为 NULL_POSITION：回退到当前录制位置。 */
    private static long resolvedStop(AeronArchive client, Recording rec) {
        long stop = rec.stopPosition();
        if (stop == AeronArchive.NULL_POSITION) {
            stop = client.getRecordingPosition(rec.recordingId());
        }
        return stop;
    }

    /** 回放成交 recording，按 [len][frame] 批量帧格式统计 SBE trade 帧数。 */
    private static long countTradeFrames(AeronArchive client, Recording rec) {
        long stop = resolvedStop(client, rec);
        if (stop == AeronArchive.NULL_POSITION || stop <= rec.startPosition()) {
            return 0L;
        }
        int replayStream = REPLAY_STREAM_BASE + (int) (rec.recordingId() % 10_000);
        AtomicLong frames = new AtomicLong();
        AtomicLong messages = new AtomicLong();
        FragmentAssembler assembler =
                new FragmentAssembler(
                        (buffer, offset, length, header) -> {
                            messages.incrementAndGet();
                            int pos = 0;
                            while (pos + IoCodec.FRAME_LENGTH_PREFIX <= length) {
                                int frameLen =
                                        ((buffer.getByte(offset + pos) & 0xFF) << 24)
                                                | ((buffer.getByte(offset + pos + 1) & 0xFF) << 16)
                                                | ((buffer.getByte(offset + pos + 2) & 0xFF) << 8)
                                                | (buffer.getByte(offset + pos + 3) & 0xFF);
                                if (frameLen <= 0
                                        || pos + IoCodec.FRAME_LENGTH_PREFIX + frameLen > length) {
                                    break;
                                }
                                frames.incrementAndGet();
                                pos += IoCodec.FRAME_LENGTH_PREFIX + frameLen;
                            }
                        });
        try (Subscription sub =
                client.replay(
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
        System.out.println("  replay messages=" + messages.get());
        return frames.get();
    }
}
