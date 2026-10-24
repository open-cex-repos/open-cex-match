package com.ocx.aeron;

/**
 * 每 symbol 的 Aeron IPC 流 ID 派生（FNV-1a 32 位，进程间一致，无需配置）。
 *
 * <p>入站流（ingress → core）、出站流（core → egress）与指标流（core → egress）各占一个区间，
 * 避免与 Aeron 系统流（0-3）及彼此冲突。symbol 变更不影响已分配流。
 * archive 流（core/egress → Archive 持久化日志）独立占区，供 OcxArchiveMain 记录；
 * WAL 流（core → Archive）与成交/深度分离，恢复走 Archive recording 回放。</p>
 */
public final class StreamIds {

    private static final int INBOUND_BASE = 2_000;
    private static final int OUTBOUND_BASE = 40_000;
    private static final int METRICS_BASE = 100_000;
    private static final int INGRESS_METRICS_BASE = 120_000;
    private static final int ARCHIVE_TRADE_BASE = 200_000;
    private static final int ARCHIVE_DEPTH_BASE = 230_000;

    /** 已退役的状态变更 WAL 流（core → Archive；常量保留防止流号复用，历史 recording 仅审计）。 */
    private static final int ARCHIVE_WAL_BASE = 260_000;

    /** 快照 recording 流（core 快照线程发布 → Archive 记录；每 snapshot 一次 start/stop）。 */
    private static final int SNAPSHOT_BASE = 280_000;

    /** DLT 死信全局流（帧内自带 symbol，低频路径）。 */
    public static final int ARCHIVE_DLT_STREAM = 300_000;

    /** ArchiveProbe 回放目的地流（避开记录流与系统流）。 */
    public static final int REPLAY_BASE = 400_000;

    /** 恢复回放目的地流（core 启动期，与 ArchiveProbe 错开）。 */
    private static final int WAL_REPLAY_BASE = 410_000;

    /** 快照 recording 回放目的地流（恢复端，与入站回放/ArchiveProbe 错开）。 */
    private static final int SNAPSHOT_REPLAY_BASE = 420_000;

    /** 备机 bridge replay 目的地流（备机 Archive 复制 recording → 备机 core 入站环）。 */
    private static final int STANDBY_REPLAY_BASE = 430_000;

    /** 备机入站流（bridge + 备机 ingress 发布 → 备机 core 订阅；备机 Archive 常驻录制）。 */
    private static final int STANDBY_INBOUND_BASE = 440_000;

    /** 入站 feed 流（ingress 发布 → journal bridge 订阅；2026-09-01 录制解耦）。 */
    private static final int INBOUND_FEED_BASE = 470_000;

    /** 入站 journal 流（journal bridge 发布 → Archive 录制；恢复/对账回放源）。 */
    private static final int INBOUND_JOURNAL_BASE = 480_000;

    /** 桥锚点查询流（core SnapshotWriter 发 maxImage → bridge 应答 journalPosAt）。 */
    private static final int BRIDGE_ANCHOR_QUERY_BASE = 510_000;

    /** 桥锚点应答流（bridge 回 [livePos][journalPos]）。 */
    private static final int BRIDGE_ANCHOR_REPLY_BASE = 511_000;

    /** 控制流（引擎 READY / 压测 GO 信号，2026-08-30 统一走 Aeron 替代文件握手；全局单流）。 */
    public static final int CONTROL_STREAM = 500_000;

    private static final int SPAN = 30_000;

    private StreamIds() {}

    public static int inbound(String symbol) {
        return INBOUND_BASE + fnv1a(symbol) % SPAN;
    }

    /**
     * 当前进程生效的入站流：备机模式（{@code -Docx.standby.inbound-stream=true}）返回
     * 独立备机入站流（bridge/备机 ingress 发布、备机 Archive 录制，避免与主机 inbound
     * recording 复制冲突）；默认返回主机入站流。恢复/对账/入站订阅统一走本方法。
     */
    public static int inboundEffective(String symbol) {
        return Boolean.parseBoolean(System.getProperty("ocx.standby.inbound-stream", "false"))
                ? standbyInbound(symbol)
                : inbound(symbol);
    }

    /**
     * 当前进程生效的入站 feed 流（ingress 发布 → bridge 订阅）。备机模式下同样使用
     * 独立 feed（备机 driver 独立，流号无冲突），与 live/journal 分离。
     */
    public static int inboundFeed(String symbol) {
        return INBOUND_FEED_BASE + fnv1a(symbol) % SPAN;
    }

    /**
     * 当前进程生效的入站 journal 流（bridge 发布 → Archive 录制；恢复/对账回放源）。
     * 与 live 流（{@link #inboundEffective}）分离：录制端卡顿不再节流撮合链路。
     */
    public static int inboundJournal(String symbol) {
        return INBOUND_JOURNAL_BASE + fnv1a(symbol) % SPAN;
    }

    public static int bridgeAnchorQuery(String symbol) {
        return BRIDGE_ANCHOR_QUERY_BASE + fnv1a(symbol) % SPAN;
    }

    public static int bridgeAnchorReply(String symbol) {
        return BRIDGE_ANCHOR_REPLY_BASE + fnv1a(symbol) % SPAN;
    }

    /** 录制/回放侧入站流：bridge 模式录 journal，兼容模式（无 bridge）录 live。 */
    public static int inboundJournalEffective(String symbol) {
        return Boolean.parseBoolean(System.getProperty("ocx.journal-bridge.enabled", "false"))
                ? inboundJournal(symbol)
                : inboundEffective(symbol);
    }

    /** 备机录制/回放侧入站流：bridge 模式录备机 journal，兼容模式录 standbyInbound。 */
    public static int standbyJournalEffective(String symbol) {
        return Boolean.parseBoolean(System.getProperty("ocx.journal-bridge.enabled", "false"))
                ? inboundJournal(symbol)
                : standbyInbound(symbol);
    }

    public static int outbound(String symbol) {
        return OUTBOUND_BASE + fnv1a(symbol) % SPAN;
    }

    public static int metrics(String symbol) {
        return METRICS_BASE + fnv1a(symbol) % SPAN;
    }

    public static int ingressMetrics(String symbol) {
        return INGRESS_METRICS_BASE + fnv1a(symbol) % SPAN;
    }

    /** archive 成交流（egress 编码后的 SBE 批量帧 → Archive 记录）。 */
    public static int archiveTrade(String symbol) {
        return ARCHIVE_TRADE_BASE + fnv1a(symbol) % SPAN;
    }

    /** archive 深度流（egress 编码后的 SBE 深度帧 → Archive 记录）。 */
    public static int archiveDepth(String symbol) {
        return ARCHIVE_DEPTH_BASE + fnv1a(symbol) % SPAN;
    }

    /** 快照 recording 流（core 快照线程发布 → Archive 记录；每 snapshot 一次 start/stop）。 */
    public static int snapshot(String symbol) {
        return SNAPSHOT_BASE + fnv1a(symbol) % SPAN;
    }

    /** WAL 持久化流（core 撮合线程 → Archive 记录；egress 不消费）。 */
    public static int archiveWal(String symbol) {
        return ARCHIVE_WAL_BASE + fnv1a(symbol) % SPAN;
    }

    /** 恢复回放目的地流（core 启动期订阅，避开 archive 记录流与系统流）。 */
    public static int walReplay(String symbol) {
        return WAL_REPLAY_BASE + fnv1a(symbol) % SPAN;
    }

    /** 快照 recording 回放目的地流（恢复端；与入站回放/ArchiveProbe 错开）。 */
    public static int snapshotReplay(String symbol) {
        return SNAPSHOT_REPLAY_BASE + fnv1a(symbol) % SPAN;
    }

    /** 备机 replay 目的地流（ReplicationIngressBridge：复制 recording → 备机 core）。 */
    public static int standbyReplay(String symbol) {
        return STANDBY_REPLAY_BASE + fnv1a(symbol) % SPAN;
    }

    /** 备机入站流（备机完整输入 journal 的录制流；与主机 inbound 流隔离，避免复制冲突）。 */
    public static int standbyInbound(String symbol) {
        return STANDBY_INBOUND_BASE + fnv1a(symbol) % SPAN;
    }

    private static int fnv1a(String s) {
        int hash = 0x811c9dc5;
        for (int i = 0; i < s.length(); i++) {
            hash ^= s.charAt(i);
            hash *= 0x01000193;
        }
        return hash & 0x7fffffff;
    }
}
