package com.ocx.aeron;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;

import org.agrona.concurrent.status.CountersReader;

/**
 * Aeron IPC 客户端门面（跨进程共享内存环）。
 *
 * <p>两种形态：{@link #embedded()} 在进程内起媒体驱动（单 JVM 开发/压测路径）；
 * {@link #connect()} 连接独立媒体驱动进程（三进程生产形态，目录由
 * {@code -Docx.aeron.dir=} 指定，默认 /tmp/ocx-aeron）。</p>
 */
public final class OcxAeron implements AutoCloseable {

    public static final String CHANNEL = "aeron:ipc";
    private static final String DEFAULT_DIR = "/tmp/ocx-aeron";

    private final MediaDriver driver;
    private final Aeron aeron;

    private OcxAeron(boolean embedded) {
        if (embedded) {
            driver = MediaDriver.launchEmbedded();
            aeron =
                    Aeron.connect(
                            new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
        } else {
            driver = null;
            aeron =
                    Aeron.connect(
                            new Aeron.Context()
                                    .aeronDirectoryName(
                                            System.getProperty("ocx.aeron.dir", DEFAULT_DIR)));
        }
    }

    public static OcxAeron embedded() {
        return new OcxAeron(true);
    }

    public static OcxAeron connect() {
        return new OcxAeron(false);
    }

    /** 当前连接的媒体驱动目录（恢复/内嵌 Archive 连接复用）。 */
    public String aeronDirectoryName() {
        return aeron.context().aeronDirectoryName();
    }

    /** 底层 Aeron 客户端（bridge/探针等需要直接创建发布订阅之外的构造）。 */
    public Aeron aeron() {
        return aeron;
    }

    public Publication addInboundPublication(String symbol) {
        return aeron.addPublication(CHANNEL, StreamIds.inboundEffective(symbol));
    }

    /** 入站 feed 发布（ingress → journal bridge；2026-09-01 录制解耦）。 */
    public Publication addInboundFeedPublication(String symbol) {
        return aeron.addPublication(CHANNEL, StreamIds.inboundFeed(symbol));
    }

    /** 入站 feed 订阅（journal bridge 消费）。 */
    public Subscription addInboundFeedSubscription(String symbol) {
        return aeron.addSubscription(CHANNEL, StreamIds.inboundFeed(symbol));
    }

    /** 入站 journal 发布（journal bridge → Archive 录制）。 */
    public Publication addInboundJournalPublication(String symbol) {
        return aeron.addPublication(CHANNEL, StreamIds.inboundJournal(symbol));
    }

    /** 桥锚点查询发布（core SnapshotWriter → bridge）。 */
    public Publication addBridgeAnchorQueryPublication(String symbol) {
        return aeron.addPublication(CHANNEL, StreamIds.bridgeAnchorQuery(symbol));
    }

    /** 桥锚点应答订阅（bridge → core SnapshotWriter）。 */
    public Subscription addBridgeAnchorReplySubscription(String symbol) {
        return aeron.addSubscription(CHANNEL, StreamIds.bridgeAnchorReply(symbol));
    }

    public Subscription addInboundSubscription(String symbol) {
        return aeron.addSubscription(CHANNEL, StreamIds.inboundEffective(symbol));
    }

    /** UDP 收单订阅（客户端/网关 → ingress；endpoint 形如 {@code host:port}）。 */
    public Subscription addUdpInboundSubscription(String udpEndpoint, String symbol) {
        return aeron.addSubscription(
                "aeron:udp?endpoint=" + udpEndpoint, StreamIds.inbound(symbol));
    }

    /** 通用订阅（bridge/replay 等自定义通道与流；IPC 用 {@link #CHANNEL}）。 */
    public Subscription addSubscription(String channel, int streamId) {
        return aeron.addSubscription(channel, streamId);
    }

    public Publication addOutboundPublication(String symbol) {
        return aeron.addPublication(CHANNEL, StreamIds.outbound(symbol));
    }

    public Subscription addOutboundSubscription(String symbol) {
        return aeron.addSubscription(CHANNEL, StreamIds.outbound(symbol));
    }

    /** 指标专用出站流（与成交/深度主流分离，保持主流严格单生产者 SPSC）。 */
    public Publication addOutboundMetricsPublication(String symbol) {
        return aeron.addPublication(CHANNEL, StreamIds.metrics(symbol));
    }

    /** ingress→core 指标流（三进程模式：入站侧 lag/TPS/拒绝合流）。 */
    public Publication addInboundMetricsPublication(String symbol) {
        return aeron.addPublication(CHANNEL, StreamIds.ingressMetrics(symbol));
    }

    public Subscription addInboundMetricsSubscription(String symbol) {
        return aeron.addSubscription(CHANNEL, StreamIds.ingressMetrics(symbol));
    }

    /** archive 成交发布（egress → Archive，Bitvavo cold-log 出站）。 */
    public Publication addArchiveTradePublication(String symbol) {
        return aeron.addPublication(CHANNEL, StreamIds.archiveTrade(symbol));
    }

    /** archive 深度发布（egress → Archive）。 */
    public Publication addArchiveDepthPublication(String symbol) {
        return aeron.addPublication(CHANNEL, StreamIds.archiveDepth(symbol));
    }

    /** 快照 recording 发布（core 快照线程 → Archive；每 snapshot 一次 start/stop recording）。 */
    public Publication addSnapshotPublication(String symbol) {
        return aeron.addPublication(CHANNEL, StreamIds.snapshot(symbol));
    }

    /** archive DLT 发布（ingress → Archive 死信流）。 */
    public Publication addArchiveDltPublication() {
        return aeron.addPublication(CHANNEL, StreamIds.ARCHIVE_DLT_STREAM);
    }

    /** 媒体驱动共享计数器（入站环积压采样：pub-pos − sub-pos）。 */
    public CountersReader countersReader() {
        return aeron.countersReader();
    }

    @Override
    public void close() {
        aeron.close();
        if (driver != null) {
            driver.close();
        }
    }
}
