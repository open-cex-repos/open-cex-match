package com.ocx.ha;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.util.TimeDuration;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 仲裁组运维工具（2026-09-01，P0-3 回切）：
 * <ul>
 *   <li>{@code --query}：查询当前 epoch/active/成员复制位置；</li>
 *   <li>{@code --switch <instanceId> [--position <n>]}：受控切换——operator 显式
 *       指定新 active（跳过租约过期检查，epoch++ 吊销旧 active），回切流程用。</li>
 * </ul>
 * 参数：{@code --peers id:host:port,...}（默认 127.0.0.1:6001-6003）、
 * {@code --group-id}（默认 ocx-ha-arbitration）。
 */
public final class HaArbitrationTool {

    private HaArbitrationTool() {}

    public static void main(String[] args) throws Exception {
        String peersSpec =
                arg(args, "--peers", "arb1:127.0.0.1:6001,arb2:127.0.0.1:6002,arb3:127.0.0.1:6003");
        String groupId = arg(args, "--group-id", "ocx-ha-arbitration");
        boolean query = has(args, "--query");
        String switchId = arg(args, "--switch", null);
        long position = Long.parseLong(arg(args, "--position", "0"));

        List<RaftPeer> peers = parsePeers(peersSpec);
        RaftGroup group =
                RaftGroup.valueOf(
                        RaftGroupId.valueOf(
                                UUID.nameUUIDFromBytes(groupId.getBytes(StandardCharsets.UTF_8))),
                        peers);
        RaftProperties props = new RaftProperties();
        RaftConfigKeys.Rpc.setType(props, SupportedRpcType.GRPC);

        try (RaftClient client =
                RaftClient.newBuilder()
                        .setRaftGroup(group)
                        .setProperties(props)
                        .setRetryPolicy(
                                RetryPolicies.retryUpToMaximumCountWithFixedSleep(
                                        10, TimeDuration.valueOf(200L, TimeUnit.MILLISECONDS)))
                        .build()) {
            if (query || switchId == null) {
                HaArbitration.Response state = sendReadOnly(client, HaArbitration.encodeQuery());
                System.out.println(
                        "EPOCH="
                                + state.epoch()
                                + " active="
                                + state.activeInstanceId()
                                + " leaseExpiryNs="
                                + state.leaseExpiryNs()
                                + " status="
                                + state.status());
                return;
            }
            HaArbitration.Response resp =
                    send(
                            client,
                            HaArbitration.encodeSwitch(
                                    Integer.parseInt(switchId), position, System.nanoTime()));
            System.out.println(
                    "SWITCH target="
                            + switchId
                            + " status="
                            + resp.status()
                            + " EPOCH="
                            + resp.epoch()
                            + " active="
                            + resp.activeInstanceId());
            if (resp.status() != HaArbitration.ST_OK) {
                System.exit(1);
            }
        }
    }

    private static HaArbitration.Response send(RaftClient client, ByteBuffer request)
            throws IOException {
        RaftClientReply reply = client.io().send(Message.valueOf(request));
        return HaArbitration.decodeResponse(reply.getMessage().getContent());
    }

    private static HaArbitration.Response sendReadOnly(RaftClient client, ByteBuffer request)
            throws IOException {
        RaftClientReply reply = client.io().sendReadOnly(Message.valueOf(request));
        return HaArbitration.decodeResponse(reply.getMessage().getContent());
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

    private static String arg(String[] args, String key, String def) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(key)) {
                return args[i + 1];
            }
        }
        return def;
    }

    private static boolean has(String[] args, String key) {
        for (String arg : args) {
            if (arg.equals(key)) {
                return true;
            }
        }
        return false;
    }
}
