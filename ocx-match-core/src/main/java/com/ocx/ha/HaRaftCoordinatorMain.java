package com.ocx.ha;

import com.ocx.aeron.ControlStream;
import com.sun.net.httpserver.HttpServer;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.TimeDuration;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 仲裁层 Raft 协调节点（2026-09-01）：每个节点 = 一个 Ratis server（共享
 * {@link HaArbitrationStateMachine}）+ UDP 网关。3 节点组中仅 Raft leader
 * 处理心跳（续租/ACK/提升），决策全部经 Raft 提交，彻底消除 HaCoordinatorMain
 * 的单点。
 *
 * <p>引擎侧 UDP 面保持不变：core 发 {@code TYPE_HEARTBEAT}，协调者回
 * {@code TYPE_LEASE_ACK}（position=epoch）；提升时发 {@code TYPE_PROMOTE}
 * （position=新 epoch），ReplicationIngressBridge 照旧消费。旧 primary 收到
 * 新 epoch 的 ACK 后 self-fencing——防双主语义与阶段 B 完全一致，只是判活从
 * 单点进程内变量变成多数派共识。</p>
 *
 * <p>参数：{@code -Docx.ha.raft.id}（本节点 peer id）、
 * {@code -Docx.ha.raft.peers}（"id:host:port,id:host:port,..." 全组地址）、
 * {@code -Docx.ha.raft.group-id}（默认 ocx-ha-arbitration）、
 * {@code -Docx.ha.raft.storage.dir}（本节点 Raft 存储目录）、
 * {@code -Docx.ha.raft.lease-ttl-ms}（默认 10000）、
 * {@code -Docx.ha.raft.renew-interval-ms}（默认 3000，心跳续租写入节流）、
 * {@code -Docx.ha.udp.listen}（本节点心跳订阅）、
 * {@code -Docx.ha.promote.endpoints}（逗号分隔的 PROMOTE 发布目标，各备机 bridge
 * 监听处；缺省回退 {@code -Docx.ha.udp.endpoint}）、
 * {@code -Docx.ha.primary.ack.endpoint} / {@code -Docx.ha.standby.ack.endpoints}
 * （ACK 目标）、{@code -Docx.ha.primary-id}（默认 1）、
 * {@code -Docx.ha.standby-ids}（逗号分隔，默认 2）、
 * {@code -Docx.ha.primary-timeout-ms}（默认 5000，primary 心跳超时即提议提升）。
 * 输出行：{@code ARB_READY}/{@code LEADER}/{@code RENEW_COMMITTED}/
 * {@code LEASE_ACK_SENT}/{@code PROMOTE_SENT}/{@code EPOCH=}。</p>
 */
public final class HaRaftCoordinatorMain {

    private static final long LOOP_MS = 50L;

    /** 只读查询节流：Raft read-only 请求有对象开销，50ms 一次在长跑中累积
     *  （60 分钟 7.2 万次曾导致协调者堆耗尽 OOM）；state 由 renew/promote 响应
     *  实时维护，查询仅作兜底刷新。 */
    private static final long QUERY_INTERVAL_MS = 2_000L;

    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private HaRaftCoordinatorMain() {}

    private static String ts() {
        return LocalTime.now().format(TS);
    }

    public static void main(String[] args) throws Exception {
        String peerId = System.getProperty("ocx.ha.raft.id", "arb1");
        String peersSpec =
                System.getProperty(
                        "ocx.ha.raft.peers",
                        "arb1:localhost:6001,arb2:localhost:6002,arb3:localhost:6003");
        String groupId = System.getProperty("ocx.ha.raft.group-id", "ocx-ha-arbitration");
        String storageDir =
                System.getProperty("ocx.ha.raft.storage.dir", "/tmp/ocx-ha-raft/" + peerId);
        long ttlMs = Long.getLong("ocx.ha.raft.lease-ttl-ms", 10_000L);
        long renewIntervalMs = Long.getLong("ocx.ha.raft.renew-interval-ms", 3_000L);
        long electionMinMs = Long.getLong("ocx.ha.raft.election-timeout-min-ms", 300L);
        long electionMaxMs = Long.getLong("ocx.ha.raft.election-timeout-max-ms", 700L);
        int metricsPort = Integer.getInteger("ocx.ha.metrics.port", 0);

        String listen = System.getProperty("ocx.ha.udp.listen", "aeron:udp?endpoint=0.0.0.0:53001");
        String promoteEndpointsSpec = System.getProperty("ocx.ha.promote.endpoints");
        if (promoteEndpointsSpec == null) {
            promoteEndpointsSpec =
                    System.getProperty("ocx.ha.udp.endpoint", "aeron:udp?endpoint=localhost:53000");
        }
        List<String> promoteChannels = splitCsv(promoteEndpointsSpec);
        String primaryAckEndpoint =
                System.getProperty(
                        "ocx.ha.primary.ack.endpoint", "aeron:udp?endpoint=localhost:53010");
        String standbyAckEndpointsSpec =
                System.getProperty(
                        "ocx.ha.standby.ack.endpoints", "aeron:udp?endpoint=localhost:53020");
        int primaryId = Integer.getInteger("ocx.ha.primary-id", 1);
        List<Integer> standbyIds = parseIds(System.getProperty("ocx.ha.standby-ids", "2"));
        long primaryTimeoutMs = Long.getLong("ocx.ha.primary-timeout-ms", 5_000L);
        String aeronDir = System.getProperty("ocx.aeron.dir", "/tmp/ocx-aeron");
        List<String> standbyAckEndpoints = splitCsv(standbyAckEndpointsSpec);

        List<RaftPeer> peers = parsePeers(peersSpec);
        RaftPeerId localId = RaftPeerId.valueOf(peerId);
        RaftGroup group =
                RaftGroup.valueOf(
                        RaftGroupId.valueOf(
                                UUID.nameUUIDFromBytes(groupId.getBytes(StandardCharsets.UTF_8))),
                        peers);

        // 仲裁存储是易失的（epoch/租约全在 Raft 日志，重启后从 leader 重同步）：
        // 遗留上次被杀进程的存储目录会让 Ratis 重开抛 FORMAT 失败——启动前清空
        // 本节点存储，保证 kill -9 后重启可恢复（2026-09-02 演练 A 实测）。
        File storageRoot = new File(storageDir);
        File[] stale = storageRoot.listFiles();
        if (stale != null) {
            for (File f : stale) {
                deleteRecursively(f);
            }
        }
        storageRoot.mkdirs();

        RaftProperties props = new RaftProperties();
        RaftConfigKeys.Rpc.setType(props, SupportedRpcType.GRPC);
        RaftServerConfigKeys.setStorageDir(props, List.of(new File(storageDir)));
        RaftServerConfigKeys.Rpc.setTimeoutMin(
                props, TimeDuration.valueOf(electionMinMs, TimeUnit.MILLISECONDS));
        RaftServerConfigKeys.Rpc.setTimeoutMax(
                props, TimeDuration.valueOf(electionMaxMs, TimeUnit.MILLISECONDS));
        // gRPC server 默认绑随机端口（port=0）：必须显式绑到本节点 peer 地址端口
        GrpcConfigKeys.Server.setPort(props, peerPort(localId, peers));

        HaArbitrationStateMachine stateMachine = new HaArbitrationStateMachine(ttlMs);
        AtomicLong promoteCount = new AtomicLong();
        AtomicLong transientErrorCount = new AtomicLong();
        RaftServer server =
                RaftServer.newBuilder()
                        .setServerId(localId)
                        .setGroup(group)
                        .setStateMachine(stateMachine)
                        .setProperties(props)
                        .build();
        server.start();
        HttpServer metricsServer =
                startMetricsServer(
                        metricsPort,
                        stateMachine,
                        server,
                        group.getGroupId(),
                        peerId,
                        primaryId,
                        standbyIds,
                        promoteCount,
                        transientErrorCount);

        RaftClient client =
                RaftClient.newBuilder()
                        .setRaftGroup(group)
                        .setProperties(props)
                        .setRetryPolicy(
                                RetryPolicies.retryUpToMaximumCountWithFixedSleep(
                                        5, TimeDuration.valueOf(200L, TimeUnit.MILLISECONDS)))
                        .build();

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
                Subscription sub =
                        aeron.addSubscription(listen, HaControlStream.HA_CONTROL_STREAM)) {
            // READY 一次性消息可能被顺序等待的编排脚本错过：周期性重发前 10s
            // （wait_ready 订阅后 1s 内必能收到）
            Thread readyPump =
                    new Thread(
                            () -> {
                                try {
                                    for (int i = 0; i < 10; i++) {
                                        ControlStream.publishReady(
                                                aeronDir, "coordinator-" + peerId);
                                        Thread.sleep(1_000L);
                                    }
                                } catch (InterruptedException ignored) {
                                    Thread.currentThread().interrupt();
                                }
                            },
                            "arb-ready-pump");
            readyPump.setDaemon(true);
            readyPump.start();
            System.err.println(
                    "["
                            + ts()
                            + "] [HaRaftCoordinator] ARB_READY id="
                            + peerId
                            + " peers="
                            + peersSpec
                            + " storage="
                            + storageDir
                            + " leaseTtlMs="
                            + ttlMs);

            HaArbitration.Response state =
                    new HaArbitration.Response(HaArbitration.ST_OK, 0L, 0, 0L);
            boolean registered = false;
            Map<Integer, Long> lastHeartbeat = new HashMap<>();
            Map<Integer, Long> lastPosition = new HashMap<>();
            Map<Integer, Long> lastRenewAt = new HashMap<>();
            Map<String, Publication> ackPubs = new HashMap<>();
            List<Integer> received = new ArrayList<>();
            long lastLeaderLog = 0L;
            long lastQueryAt = 0L;
            boolean wasLeader = false;

            IdleStrategy idle = new BackoffIdleStrategy(100, 100, 1_000, 1_000_000);
            while (true) {
                try {
                    boolean leader = isLeader(server, group.getGroupId());
                    if (leader && !wasLeader) {
                        System.err.println(
                                "[" + ts() + "] [HaRaftCoordinator] LEADER_ELECTED id=" + peerId);
                    }
                    wasLeader = leader;
                    if (leader) {
                        long now = System.nanoTime();
                        // 启动/成为 leader 后注册成员（幂等），并拉取一次仲裁状态
                        if (!registered) {
                            boolean allOk = true;
                            for (int id : allInstanceIds(primaryId, standbyIds)) {
                                HaArbitration.Response resp =
                                        send(
                                                client,
                                                HaArbitration.encodeRegister(
                                                        id,
                                                        id == primaryId
                                                                ? HaArbitration.ROLE_PRIMARY
                                                                : HaArbitration.ROLE_STANDBY,
                                                        0L));
                                allOk &= resp.status() == HaArbitration.ST_OK;
                            }
                            if (allOk) {
                                registered = true;
                                System.err.println(
                                        "["
                                                + ts()
                                                + "] [HaRaftCoordinator] REGISTERED id="
                                                + peerId);
                            }
                        }
                        long nowNs = System.nanoTime();
                        if (nowNs - lastQueryAt >= QUERY_INTERVAL_MS * 1_000_000L) {
                            state = query(client);
                            lastQueryAt = nowNs;
                        }
                        if (now - lastLeaderLog > TimeUnit.SECONDS.toNanos(10)) {
                            lastLeaderLog = now;
                            StringBuilder positions = new StringBuilder();
                            for (int id : standbyIds) {
                                positions
                                        .append(" ")
                                        .append(id)
                                        .append("=")
                                        .append(stateMachine.position(id));
                            }
                            System.err.println(
                                    "["
                                            + ts()
                                            + "] [HaRaftCoordinator] LEADER id="
                                            + peerId
                                            + " EPOCH="
                                            + state.epoch()
                                            + " active="
                                            + state.activeInstanceId()
                                            + " standbyPos:"
                                            + positions);
                        }
                    }

                    int work =
                            sub.poll(
                                    (buffer, offset, length, header) -> {
                                        if (length < HaControlStream.ROW_LEN) {
                                            return;
                                        }
                                        int type = buffer.getInt(offset);
                                        int instanceId = buffer.getInt(offset + Integer.BYTES);
                                        if (type != HaControlStream.TYPE_HEARTBEAT) {
                                            return;
                                        }
                                        lastHeartbeat.put(instanceId, System.currentTimeMillis());
                                        lastPosition.put(
                                                instanceId,
                                                buffer.getLong(
                                                        offset
                                                                + Integer.BYTES
                                                                + Integer.BYTES
                                                                + HaControlStream.NAME_LEN));
                                        received.add(instanceId);
                                    },
                                    64);

                    // leader 才裁决：续租/ACK（心跳仅记录，poll 之外处理避免 lambda 捕获可变状态）
                    if (leader) {
                        for (int instanceId : received) {
                            long nowNs = System.nanoTime();
                            // 所有已注册实例统一走 RENEW（按实例节流）：active 续租 +
                            // 备机上报复制位置（P0-1），状态机对非 active 仅更新成员表
                            long last = lastRenewAt.getOrDefault(instanceId, 0L);
                            if (nowNs - last >= renewIntervalMs * 1_000_000L) {
                                lastRenewAt.put(instanceId, nowNs);
                                state =
                                        renew(
                                                client,
                                                state,
                                                instanceId,
                                                nowNs,
                                                ttlMs,
                                                lastPosition.getOrDefault(instanceId, 0L));
                            }
                            publishAck(
                                    aeron,
                                    ackPubs,
                                    ackEndpointFor(
                                            instanceId,
                                            primaryId,
                                            standbyIds,
                                            primaryAckEndpoint,
                                            standbyAckEndpoints),
                                    instanceId,
                                    state.epoch());
                        }
                        received.clear();
                    }

                    if (leader) {
                        long nowMs = System.currentTimeMillis();
                        int active = state.activeInstanceId();
                        Long lastActive = active == 0 ? null : lastHeartbeat.get(active);
                        boolean leaseExpired = System.nanoTime() >= state.leaseExpiryNs();
                        // 必须先见过 active 至少一次心跳才允许判死（启动期租约初始为 0
                        // 视为过期，但没见过心跳不得提升——与 HaCoordinatorMain 语义一致）
                        boolean activeStale =
                                lastActive != null && nowMs - lastActive > primaryTimeoutMs;
                        if (active > 0 && leaseExpired && activeStale) {
                            int candidate = bestStandby(stateMachine, active, standbyIds);
                            if (candidate > 0) {
                                state =
                                        promote(
                                                client,
                                                state,
                                                candidate,
                                                stateMachine.position(candidate),
                                                System.nanoTime());
                                if (state.status() == HaArbitration.ST_OK) {
                                    boolean allOk = true;
                                    for (String channel : promoteChannels) {
                                        allOk &=
                                                HaControlStream.publish(
                                                        aeron,
                                                        channel,
                                                        HaControlStream.TYPE_PROMOTE,
                                                        candidate,
                                                        "promote",
                                                        state.epoch(),
                                                        3_000L);
                                    }
                                    System.err.println(
                                            "["
                                                    + ts()
                                                    + "] [HaRaftCoordinator] PROMOTE_SENT"
                                                    + " standby="
                                                    + candidate
                                                    + " EPOCH="
                                                    + state.epoch()
                                                    + " pos="
                                                    + stateMachine.position(candidate)
                                                    + " publish="
                                                    + allOk);
                                    promoteCount.incrementAndGet();
                                }
                            }
                        }
                    }
                    idle.idle(work);
                } catch (Throwable t) {
                    // 单次循环瞬态异常（Aeron CLOSING 等）不杀死仲裁节点；
                    // Error（OOM/断言失败）也记录后继续，防静默死亡（长跑 OOM 教训）
                    System.err.println(
                            "["
                                    + ts()
                                    + "] [HaRaftCoordinator] transient error id="
                                    + peerId
                                    + " err="
                                    + t);
                    transientErrorCount.incrementAndGet();
                    idle.idle(0);
                }
            }
        } finally {
            client.close();
            server.close();
            if (metricsServer != null) {
                metricsServer.stop(0);
            }
        }
    }

    /** 轻量 Prometheus /metrics 端点（JDK HttpServer，零框架依赖；端口 0 = 关闭）。
     *  暴露仲裁组健康视图供 Prometheus 抓取 + 告警规则消费。 */
    private static HttpServer startMetricsServer(
            int port,
            HaArbitrationStateMachine stateMachine,
            RaftServer server,
            RaftGroupId groupId,
            String peerId,
            int primaryId,
            List<Integer> standbyIds,
            AtomicLong promoteCount,
            AtomicLong transientErrorCount) {
        if (port <= 0) {
            return null;
        }
        try {
            HttpServer http = HttpServer.create(new InetSocketAddress(port), 0);
            http.createContext(
                    "/metrics",
                    exchange -> {
                        boolean leader = isLeader(server, groupId);
                        long epoch = stateMachine.epoch();
                        int active = stateMachine.activeInstanceId();
                        boolean leaseExpired = System.nanoTime() >= stateMachine.leaseExpiryNs();
                        StringBuilder sb = new StringBuilder(512);
                        sb.append(
                                "# HELP ocx_ha_arb_leader 1 if this arbitration node is the raft"
                                    + " leader\n");
                        sb.append("# TYPE ocx_ha_arb_leader gauge\n");
                        sb.append("ocx_ha_arb_leader{node=\"")
                                .append(peerId)
                                .append("\"} ")
                                .append(leader ? 1 : 0)
                                .append('\n');
                        sb.append("# HELP ocx_ha_arb_epoch current fencing epoch\n");
                        sb.append("# TYPE ocx_ha_arb_epoch gauge\n");
                        sb.append("ocx_ha_arb_epoch{node=\"")
                                .append(peerId)
                                .append("\"} ")
                                .append(epoch)
                                .append('\n');
                        sb.append("# HELP ocx_ha_arb_active_instance current active instance id\n");
                        sb.append("# TYPE ocx_ha_arb_active_instance gauge\n");
                        sb.append("ocx_ha_arb_active_instance{node=\"")
                                .append(peerId)
                                .append("\"} ")
                                .append(active)
                                .append('\n');
                        sb.append("# HELP ocx_ha_arb_lease_expired 1 if active lease expired\n");
                        sb.append("# TYPE ocx_ha_arb_lease_expired gauge\n");
                        sb.append("ocx_ha_arb_lease_expired{node=\"")
                                .append(peerId)
                                .append("\"} ")
                                .append(leaseExpired ? 1 : 0)
                                .append('\n');
                        sb.append(
                                "# HELP ocx_ha_arb_member_position replicated position per"
                                    + " instance\n");
                        sb.append("# TYPE ocx_ha_arb_member_position gauge\n");
                        for (int i = 1; i <= 8; i++) {
                            if (stateMachine.isMember(i)) {
                                sb.append("ocx_ha_arb_member_position{node=\"")
                                        .append(peerId)
                                        .append("\",instance=\"")
                                        .append(i)
                                        .append("\"} ")
                                        .append(stateMachine.position(i))
                                        .append('\n');
                            }
                        }
                        // 备机追赶延迟：active position - 最高备机 position（字节）
                        long activePos = stateMachine.position(stateMachine.activeInstanceId());
                        long bestStandbyPos = 0L;
                        for (int id : standbyIds) {
                            if (stateMachine.isMember(id)) {
                                bestStandbyPos =
                                        Math.max(bestStandbyPos, stateMachine.position(id));
                            }
                        }
                        long lag = Math.max(0L, activePos - bestStandbyPos);
                        sb.append(
                                "# HELP ocx_ha_arb_standby_lag active position minus best standby"
                                    + " position\n");
                        sb.append("# TYPE ocx_ha_arb_standby_lag gauge\n");
                        sb.append("ocx_ha_arb_standby_lag{node=\"")
                                .append(peerId)
                                .append("\"} ")
                                .append(lag)
                                .append('\n');
                        sb.append(
                                "# HELP ocx_ha_arb_promote_total number of promotions observed\n");
                        sb.append("# TYPE ocx_ha_arb_promote_total counter\n");
                        sb.append("ocx_ha_arb_promote_total{node=\"")
                                .append(peerId)
                                .append("\"} ")
                                .append(promoteCount.get())
                                .append('\n');
                        sb.append(
                                "# HELP ocx_ha_arb_transient_error_total transient errors"
                                    + " observed\n");
                        sb.append("# TYPE ocx_ha_arb_transient_error_total counter\n");
                        sb.append("ocx_ha_arb_transient_error_total{node=\"")
                                .append(peerId)
                                .append("\"} ")
                                .append(transientErrorCount.get())
                                .append('\n');
                        byte[] body = sb.toString().getBytes(StandardCharsets.UTF_8);
                        exchange.getResponseHeaders()
                                .set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
                        exchange.sendResponseHeaders(200, body.length);
                        exchange.getResponseBody().write(body);
                        exchange.close();
                    });
            http.start();
            System.err.println(
                    "["
                            + ts()
                            + "] [HaRaftCoordinator] METRICS_READY id="
                            + peerId
                            + " port="
                            + port);
            return http;
        } catch (Exception e) {
            System.err.println(
                    "["
                            + ts()
                            + "] [HaRaftCoordinator] metrics endpoint failed id="
                            + peerId
                            + " err="
                            + e);
            return null;
        }
    }

    private static boolean isLeader(RaftServer server, RaftGroupId groupId) {
        try {
            RaftPeerId leaderId = server.getDivision(groupId).getInfo().getLeaderId();
            return leaderId != null && leaderId.equals(server.getId());
        } catch (IOException e) {
            return false;
        }
    }

    /** 提交一条命令并返回响应；失败（无 leader/重试耗尽）返回 ST_BAD_REQUEST 占位。 */
    private static HaArbitration.Response send(RaftClient client, ByteBuffer request) {
        try {
            RaftClientReply reply = client.io().send(Message.valueOf(request));
            if (reply == null || !reply.isSuccess() || reply.getMessage() == null) {
                return new HaArbitration.Response(HaArbitration.ST_BAD_REQUEST, 0L, 0, 0L);
            }
            return HaArbitration.decodeResponse(reply.getMessage().getContent());
        } catch (IOException | RuntimeException e) {
            return new HaArbitration.Response(HaArbitration.ST_BAD_REQUEST, 0L, 0, 0L);
        }
    }

    private static HaArbitration.Response renew(
            RaftClient client,
            HaArbitration.Response state,
            int instanceId,
            long nowNs,
            long ttlMs,
            long position) {
        HaArbitration.Response resp =
                send(
                        client,
                        HaArbitration.encodeRenew(
                                instanceId,
                                state.epoch(),
                                System.nanoTime(),
                                nowNs,
                                ttlMs,
                                position));
        if (resp.status() == HaArbitration.ST_OK
                || resp.status() == HaArbitration.ST_EPOCH_MISMATCH) {
            System.err.println(
                    "["
                            + ts()
                            + "] [HaRaftCoordinator] RENEW_COMMITTED instance="
                            + instanceId
                            + " EPOCH="
                            + resp.epoch()
                            + " status="
                            + resp.status());
            return resp;
        }
        return state;
    }

    private static HaArbitration.Response promote(
            RaftClient client,
            HaArbitration.Response state,
            int candidate,
            long position,
            long proposalTimeNs) {
        HaArbitration.Response resp =
                send(client, HaArbitration.encodePromote(candidate, position, proposalTimeNs));
        if (resp.status() == HaArbitration.ST_OK) {
            return resp;
        }
        if (resp.status() == HaArbitration.ST_LEASE_ACTIVE) {
            // 其他协调者已提升或租约未到期：同步状态，本节点不再重复提议
            System.err.println(
                    "["
                            + ts()
                            + "] [HaRaftCoordinator] PROMOTE_SKIPPED status=LEASE_ACTIVE"
                            + " EPOCH="
                            + resp.epoch()
                            + " active="
                            + resp.activeInstanceId());
        }
        return resp;
    }

    private static HaArbitration.Response query(RaftClient client) {
        try {
            RaftClientReply reply =
                    client.io().sendReadOnly(Message.valueOf(HaArbitration.encodeQuery()));
            if (reply != null && reply.isSuccess() && reply.getMessage() != null) {
                return HaArbitration.decodeResponse(reply.getMessage().getContent());
            }
        } catch (IOException | RuntimeException ignored) {
            // 无 leader 时保持旧状态，心跳 ACK 照发（epoch 未变）
        }
        return new HaArbitration.Response(HaArbitration.ST_OK, 0L, 0, 0L);
    }

    private static void publishAck(
            Aeron aeron,
            Map<String, Publication> ackPubs,
            String channel,
            int instanceId,
            long epoch) {
        Publication pub = ackPubs.get(channel);
        if (pub == null) {
            pub = aeron.addPublication(channel, HaControlStream.HA_CONTROL_STREAM);
            ackPubs.put(channel, pub);
        }
        HaControlStream.publish(
                pub, HaControlStream.TYPE_LEASE_ACK, instanceId, "ack", epoch, 500L);
    }

    static int bestStandby(
            HaArbitrationStateMachine stateMachine, int activeId, List<Integer> standbyIds) {
        int best = 0;
        long bestPosition = -1L;
        for (int id : standbyIds) {
            if (id == activeId || stateMachine.role(id) != HaArbitration.ROLE_STANDBY) {
                continue;
            }
            long position = stateMachine.position(id);
            if (position > bestPosition || (position == bestPosition && (best == 0 || id < best))) {
                best = id;
                bestPosition = position;
            }
        }
        return best;
    }

    private static String ackEndpointFor(
            int instanceId,
            int primaryId,
            List<Integer> standbyIds,
            String primaryAckEndpoint,
            List<String> standbyAckEndpoints) {
        if (instanceId == primaryId) {
            return primaryAckEndpoint;
        }
        int idx = standbyIds.indexOf(instanceId);
        if (idx >= 0 && idx < standbyAckEndpoints.size()) {
            return standbyAckEndpoints.get(idx);
        }
        return standbyAckEndpoints.isEmpty() ? primaryAckEndpoint : standbyAckEndpoints.get(0);
    }

    private static List<Integer> allInstanceIds(int primaryId, List<Integer> standbyIds) {
        List<Integer> ids = new ArrayList<>();
        ids.add(primaryId);
        ids.addAll(standbyIds);
        return ids;
    }

    private static List<RaftPeer> parsePeers(String spec) {
        List<RaftPeer> peers = new ArrayList<>();
        for (String part : spec.split(",")) {
            String[] kv = part.trim().split(":");
            peers.add(
                    RaftPeer.newBuilder()
                            .setId(RaftPeerId.valueOf(kv[0]))
                            .setAddress(kv[1] + ":" + kv[2])
                            .build());
        }
        return peers;
    }

    private static int peerPort(RaftPeerId localId, List<RaftPeer> peers) {
        for (RaftPeer peer : peers) {
            if (peer.getId().equals(localId)) {
                String address = peer.getAddress();
                return Integer.parseInt(address.substring(address.lastIndexOf(':') + 1));
            }
        }
        throw new IllegalArgumentException("本地 peer id 不在集群配置中: " + localId);
    }

    private static void deleteRecursively(File f) {
        if (f.isDirectory()) {
            File[] children = f.listFiles();
            if (children != null) {
                for (File c : children) {
                    deleteRecursively(c);
                }
            }
        }
        f.delete();
    }

    private static List<Integer> parseIds(String spec) {
        List<Integer> ids = new ArrayList<>();
        for (String part : spec.split(",")) {
            ids.add(Integer.parseInt(part.trim()));
        }
        return ids;
    }

    private static List<String> splitCsv(String spec) {
        List<String> list = new ArrayList<>();
        for (String part : spec.split(",")) {
            list.add(part.trim());
        }
        return list;
    }
}
