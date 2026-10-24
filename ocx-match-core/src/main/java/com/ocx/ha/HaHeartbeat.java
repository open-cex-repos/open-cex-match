package com.ocx.ha;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * 主备心跳 + 租约（2026-09-01 fencing）：core 进程周期（1s）向协调者发 HEARTBEAT
 * （leaseRequestId 递增），等待协调者 LEASE_ACK（携带当前 epoch）。
 *
 * <p>self-fencing（fail-stop，防双主）：连续 {@link #MAX_MISSED} 次收不到 ACK
 * （协调者失联/分区）或 ACK 的 epoch 已变（本实例被取代）→ 触发 onFenced 回调
 * （core 停止撮合线程，不再对外输出）。网络分区时宁可停机，绝不双主。</p>
 *
 * <p>启动宽限（2026-09-01）：编排/部署顺序下协调者可能晚于引擎就绪，primary
 * 若从启动即刻计数失联会在协调者上线前误自杀。语义：收到首个 ACK 之前不累计
 * 失联（未确认自己是 active，无从吊销），收到首个 ACK 后立即恢复严格 fail-stop；
 * 另有硬宽限 {@code -Docx.ha.heartbeat.startup-grace-ms}（默认 30s）兜底——
 * 协调者从未上线也不至于永不自杀。</p>
 */
public final class HaHeartbeat implements Runnable {

    private static final long HEARTBEAT_MS = 1_000L;
    private static final long ACK_WAIT_MS = 500L;
    private static final int MAX_MISSED_DEFAULT = 3;
    private static final long STARTUP_GRACE_MS_DEFAULT = 30_000L;

    private final Aeron aeron;
    private final List<String> channels;
    private final String listen;
    private final int instanceId;
    private final String role;
    private final Runnable onFenced;

    /** 上报的复制位置（备机入站消费进度）；null 时回退 leaseRequestId。 */
    private final LongSupplier positionSupplier;

    private final long fenceAfterMissed;
    private final long startupGraceNs;
    private final long startedAtNs = System.nanoTime();

    private Subscription ackSub;
    private long leaseRequestId = 1L;
    private long ackedEpoch;
    private int missedAcks;
    private volatile boolean running = true;
    private volatile boolean fenced;

    public HaHeartbeat(
            String aeronDir,
            String channel,
            String listen,
            int instanceId,
            String role,
            Runnable onFenced) {
        this(aeronDir, List.of(channel), listen, instanceId, role, onFenced, null);
    }

    public HaHeartbeat(
            String aeronDir,
            List<String> channels,
            String listen,
            int instanceId,
            String role,
            Runnable onFenced) {
        this(aeronDir, channels, listen, instanceId, role, onFenced, null);
    }

    public HaHeartbeat(
            String aeronDir,
            List<String> channels,
            String listen,
            int instanceId,
            String role,
            Runnable onFenced,
            LongSupplier positionSupplier) {
        this.aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
        this.channels = channels;
        this.listen = listen;
        this.instanceId = instanceId;
        this.role = role;
        this.onFenced = onFenced;
        this.positionSupplier = positionSupplier;
        int maxMissed = Integer.getInteger("ocx.ha.heartbeat.max-missed", MAX_MISSED_DEFAULT);
        this.startupGraceNs =
                Long.getLong("ocx.ha.heartbeat.startup-grace-ms", STARTUP_GRACE_MS_DEFAULT)
                        * 1_000_000L;
        // 备机不输出：失联不自杀（无双主风险）；主机失联超阈值必须 fail-stop
        this.fenceAfterMissed = "primary".equals(role) ? maxMissed : Long.MAX_VALUE;
    }

    @Override
    public void run() {
        ackSub = aeron.addSubscription(listen, HaControlStream.HA_CONTROL_STREAM);
        System.err.println(
                "[HaHeartbeat] started instanceId="
                        + instanceId
                        + " role="
                        + role
                        + " listen="
                        + listen);
        // 常驻 publication：复用避免 add/close 频发触发 SendChannelEndpoint CLOSING
        Map<String, Publication> pubs = new HashMap<>();
        for (String channel : channels) {
            pubs.put(channel, aeron.addPublication(channel, HaControlStream.HA_CONTROL_STREAM));
        }
        boolean ackedOnce = false;
        while (running && !fenced) {
            try {
                for (String channel : channels) {
                    Publication pub = pubs.get(channel);
                    if (pub == null) {
                        pub = aeron.addPublication(channel, HaControlStream.HA_CONTROL_STREAM);
                        pubs.put(channel, pub);
                    }
                    HaControlStream.publish(
                            pub,
                            HaControlStream.TYPE_HEARTBEAT,
                            instanceId,
                            role,
                            positionSupplier == null
                                    ? leaseRequestId
                                    : positionSupplier.getAsLong(),
                            500L);
                }
                leaseRequestId++;
                if (pollAck()) {
                    missedAcks = 0;
                    ackedOnce = true;
                } else if (ackedOnce || System.nanoTime() - startedAtNs > startupGraceNs) {
                    missedAcks++;
                    if (missedAcks >= fenceAfterMissed) {
                        fence("协调者失联（连续 " + missedAcks + " 次无 ACK）");
                        break;
                    }
                }
            } catch (Exception e) {
                // 瞬态 Aeron 错误（如 SendChannelEndpoint CLOSING）：记日志继续，
                // 由 missed-ACK 计数兜底 fail-stop，绝不让心跳线程静默死亡
                System.err.println(
                        "[HaHeartbeat] transient error instanceId=" + instanceId + " err=" + e);
            }
            try {
                Thread.sleep(HEARTBEAT_MS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        aeron.close();
    }

    private boolean pollAck() {
        if (ackSub == null) {
            return false;
        }
        boolean[] got = {false};
        long deadline = System.nanoTime() + ACK_WAIT_MS * 1_000_000L;
        while (System.nanoTime() < deadline) {
            ackSub.poll(
                    (buffer, offset, length, header) -> {
                        if (length >= HaControlStream.ROW_LEN
                                && buffer.getInt(offset) == HaControlStream.TYPE_LEASE_ACK
                                && buffer.getInt(offset + Integer.BYTES) == instanceId) {
                            long epoch =
                                    buffer.getLong(
                                            offset
                                                    + Integer.BYTES
                                                    + Integer.BYTES
                                                    + HaControlStream.NAME_LEN);
                            // 吊销检查仅对 primary 生效：primary 看到 epoch 变化 = 已被取代 → 自杀；
                            // standby 看到 epoch 变化 = 被提升 → 接受新 epoch（成为事实 active）。
                            if ("primary".equals(role) && ackedEpoch != 0L && epoch != ackedEpoch) {
                                fence("epoch 被吊销（本实例已不是 active，newEpoch=" + epoch + "）");
                            } else {
                                ackedEpoch = epoch;
                                got[0] = true;
                            }
                        }
                    },
                    16);
            if (got[0] || fenced) {
                break;
            }
        }
        return got[0];
    }

    private void fence(String reason) {
        if (fenced) {
            return;
        }
        fenced = true;
        System.err.println(
                "[HaHeartbeat] SELF_FENCED instanceId="
                        + instanceId
                        + " role="
                        + role
                        + " reason="
                        + reason);
        if (onFenced != null) {
            onFenced.run();
        }
    }

    public boolean isFenced() {
        return fenced;
    }

    public void stop() {
        running = false;
        aeron.close();
    }
}
