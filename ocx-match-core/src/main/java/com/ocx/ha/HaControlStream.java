package com.ocx.ha;

import io.aeron.Aeron;
import io.aeron.Publication;

import org.agrona.concurrent.UnsafeBuffer;

import java.nio.charset.StandardCharsets;

/**
 * 主备 HA 控制流（2026-09-01 阶段 B，独立于 {@link ControlStream} 编排流）：
 * 实例心跳 / 提升指令。**UDP 通道**（主备媒体驱动目录相互独立，协调者连 UDP
 * 即可同时收主备心跳；生产跨机同样适用）。行格式 32B：
 * {@code [4B type][4B instanceId][16B name][8B position]}。
 *
 * <p>租约协议（2026-09-01 fencing）：引擎 core 周期发
 * type={@link #TYPE_HEARTBEAT}(1)（name=角色，position=leaseRequestId 递增序号），
 * 协调者回复 type={@link #TYPE_LEASE_ACK}(4)（instanceId=主/备 id，position=当前
 * epoch）；主机的续租响应里 epoch 若已变（被取代）或连续无 ACK，必须 self-fencing
 * 停止撮合（fail-stop，防双主）。type={@link #TYPE_PROMOTE}(2)：协调者发布，
 * instanceId=目标备机，position=新 epoch；type={@link #TYPE_DEMOTE}(3)：预留。</p>
 */
public final class HaControlStream {

    public static final int HA_CONTROL_STREAM = 500_001;
    public static final int TYPE_HEARTBEAT = 1;
    public static final int TYPE_PROMOTE = 2;
    public static final int TYPE_DEMOTE = 3;
    public static final int TYPE_LEASE_ACK = 4;
    public static final int NAME_LEN = 16;
    public static final int ROW_LEN = Integer.BYTES + Integer.BYTES + NAME_LEN + Long.BYTES;

    private HaControlStream() {}

    public static int encode(
            UnsafeBuffer buf, int type, int instanceId, String name, long position) {
        buf.putInt(0, type);
        buf.putInt(Integer.BYTES, instanceId);
        putName(buf, Integer.BYTES + Integer.BYTES, name);
        buf.putLong(Integer.BYTES + Integer.BYTES + NAME_LEN, position);
        return ROW_LEN;
    }

    private static void putName(UnsafeBuffer buf, int offset, String name) {
        byte[] raw = name.getBytes(StandardCharsets.US_ASCII);
        buf.setMemory(offset, NAME_LEN, (byte) 0);
        buf.putBytes(offset, raw, 0, Math.min(NAME_LEN, raw.length));
    }

    /** 发布一条 HA 控制行（带重试，超时返回 false；尽力而为，不阻塞调用方）。 */
    public static boolean publish(
            Aeron aeron,
            String channel,
            int type,
            int instanceId,
            String name,
            long position,
            long timeoutMs) {
        try (Publication publication = aeron.addPublication(channel, HA_CONTROL_STREAM)) {
            return publish(publication, type, instanceId, name, position, timeoutMs);
        }
    }

    /** 在既有 Publication 上发布一行（高频路径用：心跳/ACK 常驻 publication 复用，
     * 避免每次 addPublication/close 触发 SendChannelEndpoint CLOSING 瞬态错误）。 */
    public static boolean publish(
            Publication publication,
            int type,
            int instanceId,
            String name,
            long position,
            long timeoutMs) {
        UnsafeBuffer buf = new UnsafeBuffer(new byte[ROW_LEN]);
        int len = encode(buf, type, instanceId, name, position);
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (publication.offer(buf, 0, len) < 0L) {
            if (System.currentTimeMillis() > deadline) {
                return false;
            }
            Thread.onSpinWait();
        }
        return true;
    }
}
