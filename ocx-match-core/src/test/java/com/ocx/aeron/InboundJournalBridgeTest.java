package com.ocx.aeron;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 入站录制解耦桥（2026-09-01）：feed → 环 → live/journal 双路转发、耐久锚点计数器、
 * 环满回压三件事的最小闭环验证（内嵌驱动 + Archive，流号用独立 symbol 避免测试冲突）。
 */
class InboundJournalBridgeTest {

    private static final String SYMBOL = "BRIDGETST";
    private static final int RING = 1 << 16;

    static {
        // 与生产压测配置对齐：初始流控窗口 4MB（默认 128KB 会让窗口扩展节奏拖慢测试）
        System.setProperty("aeron.rcv.initial.window.length", "4194304");
        System.setProperty("aeron.socket.so_rcvbuf", "8388608");
    }

    private OcxAeron aeron;
    private Archive archive;
    private InboundJournalBridge bridge;

    @AfterEach
    void tearDown() {
        if (bridge != null) {
            bridge.close();
        }
        if (aeron != null) {
            aeron.close();
        }
        if (archive != null) {
            archive.close();
        }
    }

    @Test
    void forwardsFeedToLiveAndJournalInOrder(@TempDir File dir) {
        aeron = OcxAeron.embedded();
        archive =
                Archive.launch(
                        new Archive.Context()
                                .aeronDirectoryName(aeron.aeronDirectoryName())
                                .archiveDir(new File(dir, "archive"))
                                .controlChannel("aeron:udp?endpoint=localhost:8010")
                                .replicationChannel("aeron:udp?endpoint=localhost:8011"));
        try (AeronArchive client =
                AeronArchive.connect(
                        new AeronArchive.Context()
                                .aeronDirectoryName(aeron.aeronDirectoryName())
                                .controlRequestChannel("aeron:udp?endpoint=localhost:8010")
                                .controlResponseChannel("aeron:udp?endpoint=localhost:0"))) {
            client.startRecording(
                    OcxAeron.CHANNEL, StreamIds.inboundJournal(SYMBOL), SourceLocation.LOCAL);
        }

        Publication feedPub = aeron.addInboundFeedPublication(SYMBOL);
        Subscription liveSub = aeron.addInboundSubscription(SYMBOL);
        Subscription journalSub =
                aeron.addSubscription(OcxAeron.CHANNEL, StreamIds.inboundJournal(SYMBOL));
        bridge =
                new InboundJournalBridge(
                        aeron.aeron(),
                        aeron.addInboundFeedSubscription(SYMBOL),
                        aeron.addInboundPublication(SYMBOL),
                        aeron.addInboundJournalPublication(SYMBOL),
                        SYMBOL,
                        RING);
        bridge.start();

        // 变长行（与生产一致的 [8B ts][帧] 布局）
        byte[][] rows = {
            row(1L, 60), row(2L, 101), row(3L, 13), row(4L, 77),
        };
        UnsafeBuffer buf = new UnsafeBuffer(new byte[256]);
        for (byte[] r : rows) {
            buf.putBytes(0, r);
            while (feedPub.offer(buf, 0, r.length) < 0L) {
                Thread.onSpinWait();
            }
        }

        List<byte[]> liveGot = pollRows(liveSub, rows.length);
        assertEquals(rows.length, liveGot.size(), "live 流应收到全部行");
        for (int i = 0; i < rows.length; i++) {
            assertArrayEquals(rows[i], liveGot.get(i), "live 序#" + i);
        }
        List<byte[]> journalGot = pollRows(journalSub, rows.length);
        assertEquals(rows.length, journalGot.size(), "journal 流应收到全部行");
        for (int i = 0; i < rows.length; i++) {
            assertArrayEquals(rows[i], journalGot.get(i), "journal 序#" + i);
        }

        // 耐久锚点计数器推进（Archive 已录 journal）
        long[] durable = awaitDurability();
        assertTrue(durable[0] > 0L, "durableLivePos 应推进");
        assertTrue(durable[1] > 0L, "durableJournalPos 应推进");
        assertTrue(durable[0] >= 4L * 8L, "live 耐久位置应覆盖全部行（至少 4 行头）");
    }

    @Test
    void ringFullBackpressuresThenResumeZeroLoss() {
        aeron = OcxAeron.embedded();
        Publication feedPub = aeron.addInboundFeedPublication(SYMBOL + "2");
        Subscription liveSub = aeron.addInboundSubscription(SYMBOL + "2");
        Subscription journalSub =
                aeron.addSubscription(OcxAeron.CHANNEL, StreamIds.inboundJournal(SYMBOL + "2"));
        bridge =
                new InboundJournalBridge(
                        aeron.aeron(),
                        aeron.addInboundFeedSubscription(SYMBOL + "2"),
                        aeron.addInboundPublication(SYMBOL + "2"),
                        aeron.addInboundJournalPublication(SYMBOL + "2"),
                        SYMBOL + "2",
                        RING);
        bridge.start();
        // 无 live/journal 消费者：环填满后 feed offer 必须回压（负值，绝不静默丢行）
        UnsafeBuffer buf = new UnsafeBuffer(new byte[256]);
        int n = 0;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        boolean sawBackpressure = false;
        while (!sawBackpressure && System.nanoTime() < deadline) {
            long r = feedPub.offer(buf, 0, 64);
            if (r < 0L) {
                sawBackpressure = true;
            } else {
                n++;
            }
            Thread.onSpinWait();
        }
        assertTrue(sawBackpressure, "环满后 feed 应回压（offer<0）");
        assertTrue(n > RING / 100, "回压前应已灌入可观数据（n=" + n + "）");
        // 恢复消费：全部 n 行必须按序到达（回压=不丢，drop 缺陷回归）
        List<byte[]> got = new ArrayList<>();
        List<byte[]> gotJ = new ArrayList<>();
        deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while ((got.size() < n || gotJ.size() < n) && System.nanoTime() < deadline) {
            got.addAll(pollRows(liveSub, n - got.size()));
            gotJ.addAll(pollRows(journalSub, n - gotJ.size()));
            Thread.onSpinWait();
        }
        assertEquals(n, got.size(), "live 零丢失");
        assertEquals(n, gotJ.size(), "journal 零丢失");
        assertTrue(bridge.fillBytes() < (1 << 16), "环应收敛到近空（fill=" + bridge.fillBytes() + "）");
    }

    private static byte[] row(long ts, int len) {
        byte[] r = new byte[8 + len];
        for (int i = 0; i < 8; i++) {
            r[i] = (byte) (ts >>> (56 - i * 8));
        }
        for (int i = 0; i < len; i++) {
            r[8 + i] = (byte) (i + 1);
        }
        return r;
    }

    private static List<byte[]> pollRows(Subscription sub, int expect) {
        List<byte[]> out = new ArrayList<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (out.size() < expect && System.nanoTime() < deadline) {
            sub.poll(
                    (buffer, offset, length, header) -> {
                        byte[] copy = new byte[length];
                        buffer.getBytes(offset, copy, 0, length);
                        out.add(copy);
                    },
                    256);
            Thread.onSpinWait();
        }
        return out;
    }

    private long[] awaitDurability() {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            long[] d = JournalDurability.read(aeron.countersReader(), SYMBOL);
            if (d != null && d[0] > 0L && d[1] > 0L) {
                return d;
            }
            Thread.onSpinWait();
        }
        return new long[] {-1L, -1L};
    }
}
