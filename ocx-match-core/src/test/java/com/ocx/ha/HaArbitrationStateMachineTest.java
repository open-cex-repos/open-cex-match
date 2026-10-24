package com.ocx.ha;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 仲裁层 Raft 状态机集成验证（2026-09-01）：3 节点进程内 Ratis 组，
 * 验证 REGISTER/RENEW/PROMOTE 的完整租约-epoch 语义与防双主不变量。
 */
class HaArbitrationStateMachineTest {

    private static final long TTL_MS = 400L;
    private static final long ELECTION_MIN_MS = 300L;
    private static final long ELECTION_MAX_MS = 700L;
    private static final int PRIMARY = 1;
    private static final int STANDBY_A = 2;
    private static final int STANDBY_B = 3;

    @TempDir Path tempDir;

    @Test
    void leaseEpochSemanticsOnThreeNodeGroup() throws Exception {
        List<RaftPeer> peers = peers(3);
        RaftGroup group =
                RaftGroup.valueOf(
                        RaftGroupId.valueOf(UUID.nameUUIDFromBytes("ocx-ha-test".getBytes())),
                        peers);

        List<HaArbitrationStateMachine> stateMachines = new ArrayList<>();
        List<RaftServer> servers = new ArrayList<>();
        try {
            for (int i = 0; i < 3; i++) {
                HaArbitrationStateMachine sm = new HaArbitrationStateMachine(TTL_MS);
                stateMachines.add(sm);
                RaftServer server =
                        RaftServer.newBuilder()
                                .setServerId(peers.get(i).getId())
                                .setGroup(group)
                                .setStateMachine(sm)
                                .setProperties(properties(i, peerPort(peers.get(i))))
                                .build();
                server.start();
                servers.add(server);
            }

            try (RaftClient client = client(group, properties(0, peerPort(peers.get(0))))) {
                HaArbitration.Response state = waitForLeader(client);

                // 1) 注册成员：primary 注册即初始 active（epoch=1）
                HaArbitration.Response regPrimary =
                        register(client, PRIMARY, HaArbitration.ROLE_PRIMARY);
                assertEquals(HaArbitration.ST_OK, regPrimary.status());
                assertEquals(1L, regPrimary.epoch(), "primary 注册响应应携带 epoch=1");
                assertEquals(PRIMARY, regPrimary.activeInstanceId());
                assertEquals(
                        HaArbitration.ST_OK,
                        register(client, STANDBY_A, HaArbitration.ROLE_STANDBY).status());
                assertEquals(
                        HaArbitration.ST_OK,
                        register(client, STANDBY_B, HaArbitration.ROLE_STANDBY).status());
                state = query(client);
                assertEquals(1L, state.epoch(), "查询状态 epoch 应为 1");
                assertEquals(PRIMARY, state.activeInstanceId(), "查询状态 active 应为 primary");

                // 2) primary 续租：租约生效
                long t0 = System.nanoTime();
                state = renew(client, PRIMARY, 1L, t0);
                assertEquals(HaArbitration.ST_OK, state.status());
                assertTrue(state.leaseExpiryNs() > t0, "续租后租约截止应在未来");

                // 3) 租约有效期内提升必须被拒（防"活着的 primary 被偷家"）
                HaArbitration.Response rejected = promote(client, STANDBY_A, System.nanoTime());
                assertEquals(HaArbitration.ST_LEASE_ACTIVE, rejected.status());
                assertEquals(1L, rejected.epoch());
                assertEquals(PRIMARY, rejected.activeInstanceId());

                // 4) 租约过期后提升：epoch++，active 换人
                Thread.sleep(TTL_MS + 200L);
                state = promote(client, STANDBY_A, System.nanoTime());
                assertEquals(HaArbitration.ST_OK, state.status());
                assertEquals(2L, state.epoch());
                assertEquals(STANDBY_A, state.activeInstanceId());

                // 5) 旧 primary 不再是 active：续租返回当前状态（OK），但响应携带
                //    新 epoch——引擎侧 HaHeartbeat 的 ACK 吊销检查据此 self-fence
                state = renew(client, PRIMARY, 1L, System.nanoTime());
                assertEquals(HaArbitration.ST_OK, state.status(), "非 active 成员续租按成员确认返回当前状态");
                assertEquals(2L, state.epoch(), "吊销响应必须携带新 epoch");

                // 6) 新 active 的租约立即生效：同轮第二个提升被拒（防连升/双主窗口）
                assertEquals(
                        HaArbitration.ST_LEASE_ACTIVE,
                        promote(client, STANDBY_B, System.nanoTime()).status());

                // 7) 新 active 正常续租
                assertEquals(
                        HaArbitration.ST_OK,
                        renew(client, STANDBY_A, 2L, System.nanoTime()).status());

                // 8) 备机 position 持续上报：成员表实时更新（P0-1 选优依据）
                assertEquals(
                        HaArbitration.ST_OK,
                        renew(client, STANDBY_A, 2L, System.nanoTime(), 1_000L).status());
                assertEquals(
                        HaArbitration.ST_OK,
                        renew(client, STANDBY_B, 2L, System.nanoTime(), 2_000L).status());

                // 9) 三节点状态一致（多数派复制 apply 到 follower）
                long deadline = System.currentTimeMillis() + 5_000L;
                boolean allApplied = false;
                while (System.currentTimeMillis() < deadline) {
                    allApplied = true;
                    for (HaArbitrationStateMachine sm : stateMachines) {
                        if (sm.epoch() != 2L
                                || sm.activeInstanceId() != STANDBY_A
                                || sm.position(STANDBY_B) != 2_000L) {
                            allApplied = false;
                            break;
                        }
                    }
                    if (allApplied) {
                        break;
                    }
                    Thread.sleep(50L);
                }
                assertTrue(allApplied, "三节点应在 5s 内 apply 到同一状态（epoch=2 active=2 position=2000）");

                // 10) 提升候选选优：position 最高者胜出（P0-1）
                assertEquals(
                        STANDBY_B,
                        HaRaftCoordinatorMain.bestStandby(
                                stateMachines.get(0), STANDBY_A, List.of(STANDBY_A, STANDBY_B)),
                        "双备机 position 不同时应选追得最齐的备机");

                // 11) 受控切换（P0-3 回切）：租约有效期内 operator 也可切换，
                //     epoch++ 吊销旧 active
                HaArbitration.Response switched =
                        send(client, HaArbitration.encodeSwitch(STANDBY_B, 0L, System.nanoTime()));
                assertEquals(HaArbitration.ST_OK, switched.status());
                assertEquals(3L, switched.epoch());
                assertEquals(STANDBY_B, switched.activeInstanceId());

                // 旧 active（STANDBY_A）用旧 epoch 续租：响应携带新 epoch（吊销信号）
                HaArbitration.Response oldActiveRenew =
                        renew(client, STANDBY_A, 2L, System.nanoTime());
                assertEquals(HaArbitration.ST_OK, oldActiveRenew.status());
                assertEquals(3L, oldActiveRenew.epoch(), "受控切换后旧 active 续租响应必须携带新 epoch");
            }
        } finally {
            for (RaftServer server : servers) {
                try {
                    server.close();
                } catch (IOException ignored) {
                    // 清理失败不影响断言结果
                }
            }
        }
    }

    private HaArbitration.Response waitForLeader(RaftClient client) throws Exception {
        long deadline = System.currentTimeMillis() + 15_000L;
        IOException last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                HaArbitration.Response state = query(client);
                if (state.status() == HaArbitration.ST_OK) {
                    return state;
                }
            } catch (IOException e) {
                last = e;
            }
            Thread.sleep(100L);
        }
        throw new AssertionError("仲裁组未在 15s 内选出 leader", last);
    }

    private static HaArbitration.Response register(RaftClient client, int instanceId, byte role)
            throws IOException {
        return send(client, HaArbitration.encodeRegister(instanceId, role, 0L));
    }

    private static HaArbitration.Response renew(
            RaftClient client, int instanceId, long epoch, long nowNs) throws IOException {
        return renew(client, instanceId, epoch, nowNs, 0L);
    }

    private static HaArbitration.Response renew(
            RaftClient client, int instanceId, long epoch, long nowNs, long position)
            throws IOException {
        return send(
                client,
                HaArbitration.encodeRenew(
                        instanceId, epoch, System.nanoTime(), nowNs, TTL_MS, position));
    }

    private static HaArbitration.Response promote(
            RaftClient client, int instanceId, long proposalTimeNs) throws IOException {
        return send(client, HaArbitration.encodePromote(instanceId, 0L, proposalTimeNs));
    }

    private static HaArbitration.Response query(RaftClient client) throws IOException {
        return sendReadOnly(client, HaArbitration.encodeQuery());
    }

    private static HaArbitration.Response send(RaftClient client, java.nio.ByteBuffer request)
            throws IOException {
        RaftClientReply reply = client.io().send(Message.valueOf(request));
        return HaArbitration.decodeResponse(reply.getMessage().getContent());
    }

    private static HaArbitration.Response sendReadOnly(
            RaftClient client, java.nio.ByteBuffer request) throws IOException {
        RaftClientReply reply = client.io().sendReadOnly(Message.valueOf(request));
        return HaArbitration.decodeResponse(reply.getMessage().getContent());
    }

    private static List<RaftPeer> peers(int n) throws IOException {
        List<RaftPeer> peers = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            peers.add(
                    RaftPeer.newBuilder()
                            .setId(RaftPeerId.valueOf("arb" + (i + 1)))
                            .setAddress("127.0.0.1:" + freePort())
                            .build());
        }
        return peers;
    }

    private RaftProperties properties(int index, int grpcPort) throws IOException {
        RaftProperties props = new RaftProperties();
        RaftConfigKeys.Rpc.setType(props, SupportedRpcType.GRPC);
        GrpcConfigKeys.Server.setPort(props, grpcPort);
        RaftServerConfigKeys.setStorageDir(
                props, List.of(tempDir.resolve("raft-" + index).toFile()));
        RaftServerConfigKeys.Rpc.setTimeoutMin(
                props, TimeDuration.valueOf(ELECTION_MIN_MS, TimeUnit.MILLISECONDS));
        RaftServerConfigKeys.Rpc.setTimeoutMax(
                props, TimeDuration.valueOf(ELECTION_MAX_MS, TimeUnit.MILLISECONDS));
        return props;
    }

    private static RaftClient client(RaftGroup group, RaftProperties props) throws IOException {
        return RaftClient.newBuilder()
                .setRaftGroup(group)
                .setProperties(props)
                .setRetryPolicy(
                        RetryPolicies.retryUpToMaximumCountWithFixedSleep(
                                20, TimeDuration.valueOf(200L, TimeUnit.MILLISECONDS)))
                .build();
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static int peerPort(RaftPeer peer) {
        String address = peer.getAddress();
        return Integer.parseInt(address.substring(address.lastIndexOf(':') + 1));
    }
}
