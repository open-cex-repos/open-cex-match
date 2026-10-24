package com.ocx.egress;

import com.ocx.aeron.AeronWire;
import com.ocx.aeron.ControlStream;
import com.ocx.aeron.OcxAeron;
import com.ocx.aeron.StreamIds;
import com.ocx.config.ConfigLoader;
import com.ocx.config.MatchEngineConfig;
import com.ocx.ha.HaControlStream;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * 备机输入桥（主备热备，2026-09-01）：对备机 Archive 复制出的主机入站 recording
 * 做 **live replay**（从 recording 起点，跟随复制增长），把 264B 入站行原样转发到
 * 备机 core 的 IPC 入站环（{@code aeron:ipc}）。2026-09-02 接入桥模式：bridge 模式
 * 发布到备机 feed 流（{@link StreamIds#inboundFeed}），由备机 journal bridge 转发
 * live（standbyInbound）+ journal（inboundJournal）；兼容模式直发 standbyInbound。
 *
 * <p>备机 core 无需任何改动：它看到的输入序列 = 主机入站 recording 的完整回放，
 * 确定性撮合保证与主机状态一致。M1 从 recording 起点陪跑（主备同起点）；
 * 快照锚点追赶（备机晚启动）为后续工作。</p>
 *
 * <p>参数：{@code -Docx.aeron.dir}（备机媒体驱动目录）、
 * {@code -Docx.standby.archive.control=}（备机本机 Archive 控制通道，默认
 * localhost:18010）、{@code -Docx.standby.symbols=}（逗号分隔，缺省读引擎配置）、
 * {@code -Docx.ha.instance-id=}（备机实例号，默认 2；收到协调者 PROMOTE 后停止
 * 转发并移交输入权给备机 ingress，阶段 B 自动接管）。</p>
 */
public final class ReplicationIngressBridge {

    private static final CountDownLatch LATCH = new CountDownLatch(1);
    private static final int FRAGMENT_LIMIT = 256;

    private ReplicationIngressBridge() {}

    public static void main(String[] args) throws InterruptedException {
        String aeronDir = System.getProperty("ocx.aeron.dir", "/tmp/ocx-aeron");
        String archiveControl =
                System.getProperty(
                        "ocx.standby.archive.control", "aeron:udp?endpoint=localhost:18010");
        int instanceId = Integer.getInteger("ocx.ha.instance-id", 2);
        boolean bridgeEnabled =
                Boolean.parseBoolean(System.getProperty("ocx.journal-bridge.enabled", "false"));
        String haListen =
                System.getProperty("ocx.ha.udp.listen", "aeron:udp?endpoint=0.0.0.0:53000");
        Set<String> symbols = resolveSymbols(args);

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
                AeronArchive archive =
                        AeronArchive.connect(
                                new AeronArchive.Context()
                                        .aeronDirectoryName(aeronDir)
                                        .controlRequestChannel(archiveControl)
                                        .controlResponseChannel("aeron:udp?endpoint=localhost:0"));
                Subscription haSub =
                        aeron.addSubscription(haListen, HaControlStream.HA_CONTROL_STREAM)) {

            List<SymbolBridge> bridges = new ArrayList<>();
            for (String symbol : symbols) {
                bridges.add(new SymbolBridge(aeron, archive, symbol, bridgeEnabled));
            }

            System.err.println(
                    "[ReplicationIngressBridge] BRIDGE_READY aeron="
                            + aeronDir
                            + " archiveControl="
                            + archiveControl
                            + " symbols="
                            + symbols);
            ControlStream.publishReady(aeronDir, "bridge");

            IdleStrategy idle = new BackoffIdleStrategy(100, 100, 1_000, 1_000_000);
            boolean[] promoted = {false};
            long[] promotedEpoch = {0L};
            while (true) {
                int work = 0;
                if (!promoted[0]) {
                    work +=
                            haSub.poll(
                                    (buffer, offset, length, header) -> {
                                        if (length >= HaControlStream.ROW_LEN
                                                && buffer.getInt(offset)
                                                        == HaControlStream.TYPE_PROMOTE) {
                                            int target = buffer.getInt(offset + Integer.BYTES);
                                            if (target == instanceId || target == 0) {
                                                promoted[0] = true;
                                                promotedEpoch[0] =
                                                        buffer.getLong(
                                                                offset
                                                                        + Integer.BYTES
                                                                        + Integer.BYTES
                                                                        + HaControlStream.NAME_LEN);
                                            }
                                        }
                                    },
                                    64);
                    if (promoted[0]) {
                        for (SymbolBridge bridge : bridges) {
                            bridge.close();
                        }
                        System.err.println(
                                "[ReplicationIngressBridge] STANDBY_PROMOTED"
                                        + " instanceId="
                                        + instanceId
                                        + " EPOCH="
                                        + promotedEpoch[0]
                                        + " —— 停止复制转发，输入权移交备机 ingress");
                    }
                }
                for (SymbolBridge bridge : bridges) {
                    work += bridge.poll();
                }
                idle.idle(work);
            }
        }
    }

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

    /** 单 symbol 桥：replay 订阅 → 入站 IPC publication（264B 行原样转发）。 */
    private static final class SymbolBridge {
        private final Aeron aeron;
        private final AeronArchive archive;
        private final String symbol;
        private final boolean bridgeEnabled;
        private final int streamId;
        private long recordingId = -1L;
        private Subscription replaySub;
        private Publication inboundPub;
        private long lastRetryMs;
        private volatile boolean active = true;

        private SymbolBridge(
                Aeron aeron, AeronArchive archive, String symbol, boolean bridgeEnabled) {
            this.aeron = aeron;
            this.archive = archive;
            this.symbol = symbol;
            this.bridgeEnabled = bridgeEnabled;
            // 桥模式主机录 journal 流（备机复制同流）；兼容模式录 live 流
            this.streamId = StreamIds.inboundJournalEffective(symbol);
        }

        int poll() {
            if (!active) {
                return 0;
            }
            if (replaySub == null) {
                tryEstablish();
                return 0;
            }
            return replaySub.poll(
                    (buffer, offset, length, header) -> {
                        if (length < AeronWire.INBOUND_TS_LENGTH
                                || length > AeronWire.INBOUND_ROW_SIZE) {
                            System.err.println("[ReplicationIngressBridge] 非法入站行丢弃: len=" + length);
                            return;
                        }
                        long result;
                        while ((result = inboundPub.offer(buffer, offset, length)) < 0L) {
                            // 背压：备机 core 消费慢时自旋等待（不丢数据）
                            Thread.onSpinWait();
                        }
                    },
                    FRAGMENT_LIMIT);
        }

        void close() {
            active = false;
            if (replaySub != null) {
                replaySub.close();
            }
            if (inboundPub != null) {
                inboundPub.close();
            }
        }

        /**
         * 复制 recording 有数据后建立 live replay（从起点 0 跟随增长）并转发到备机
         * 入站环。数据未流动（主机空闲/GO 前）时每秒重试一次——常驻进程，不阻塞 READY。
         */
        private void tryEstablish() {
            long now = System.currentTimeMillis();
            if (now - lastRetryMs < 1_000L) {
                return;
            }
            lastRetryMs = now;
            try {
                if (recordingId < 0L) {
                    // 复制 recording 尚未出现在备机目录：StandbyArchiveMain 刚启动/复制中
                    recordingId = findLatestRecording(archive, streamId);
                    if (recordingId < 0L) {
                        return;
                    }
                }
                long pos = archive.getRecordingPosition(recordingId);
                if (pos == AeronArchive.NULL_POSITION || pos <= 0L) {
                    return;
                }
                replaySub =
                        archive.replay(
                                recordingId,
                                0L,
                                AeronArchive.NULL_POSITION,
                                OcxAeron.CHANNEL,
                                StreamIds.standbyReplay(symbol));
                inboundPub =
                        aeron.addPublication(
                                OcxAeron.CHANNEL,
                                bridgeEnabled
                                        ? StreamIds.inboundFeed(symbol)
                                        : StreamIds.standbyInbound(symbol));
                System.err.println(
                        "[ReplicationIngressBridge] bridge started symbol="
                                + symbol
                                + " recording="
                                + recordingId
                                + " replayStream="
                                + StreamIds.standbyReplay(symbol)
                                + " startPos=0 livePosition="
                                + pos);
            } catch (ArchiveException e) {
                System.err.println("[ReplicationIngressBridge] replay 暂不可用（重试）: " + e.getMessage());
            }
        }
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
