package com.ocx.ha;

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * 仲裁层 Raft 的命令/响应编码（2026-09-01）。
 *
 * <p>仲裁组只承载三类状态：{@code epoch}（fencing token）、{@code activeInstanceId}
 * （当前 active 实例）、{@code leaseExpiryNs}（租约截止，单调时钟域）。撮合日志
 * 不经过 Raft——仲裁层只回答"谁是 active、epoch 是多少、租约是否有效"。</p>
 *
 * <p>手工 ByteBuffer 编码（不引 SBE）：请求/响应都是几十字节级小消息。
 * 时钟域约定：{@code nowNs}/{@code proposalTimeNs} 由提议方（Raft leader 所在
 * 协调节点）注入，随日志复制，三节点 apply 到完全相同的值——租约判定确定性。
 * </p>
 */
public final class HaArbitration {

    /** 注册实例（进成员表；首个 primary 注册即成为初始 active，epoch 从 1 起）。 */
    public static final byte OP_REGISTER = 1;

    /** 续租：仅当前 active 且 epoch 匹配才延长租约；epoch 不匹配返回吊销状态。 */
    public static final byte OP_RENEW = 2;

    /** 提升：租约已过期（proposalTimeNs &gt;= leaseExpiryNs）才提交，epoch++ 换 active。 */
    public static final byte OP_PROMOTE = 3;

    /** 只读查询（走 StateMachine.query，不进日志）。 */
    public static final byte OP_QUERY = 4;

    /** 受控切换（operator 显式指定新 active，绕过租约过期检查；epoch++ 吊销旧主）。
     *  回切流程用：旧主追赶完成后由运维工具触发。 */
    public static final byte OP_SWITCH = 5;

    public static final byte ROLE_PRIMARY = 1;
    public static final byte ROLE_STANDBY = 2;

    public static final byte ST_OK = 0;

    /** RENEW 的 epoch 与当前不符：本实例已被取代（吊销信号）。 */
    public static final byte ST_EPOCH_MISMATCH = 1;

    /** 实例未注册。 */
    public static final byte ST_UNKNOWN_INSTANCE = 2;

    /** PROMOTE 被拒：active 租约仍有效。 */
    public static final byte ST_LEASE_ACTIVE = 3;

    /** PROMOTE 目标已经是 active。 */
    public static final byte ST_ALREADY_ACTIVE = 4;

    /** 请求无法解析。 */
    public static final byte ST_BAD_REQUEST = 5;

    /** REGISTER: op(1) role(1) instanceId(4) position(8)。 */
    public static final int REGISTER_LEN = 1 + 1 + Integer.BYTES + Long.BYTES;

    /** RENEW: op(1) instanceId(4) epoch(8) seq(8) nowNs(8) ttlMs(4) position(8)。
     *  position 为上报的复制位置（备机入站消费进度），随心跳持续更新成员表，
     *  供提升候选按"追得最齐"选优。 */
    public static final int RENEW_LEN = 1 + Integer.BYTES + Long.BYTES * 4 + Integer.BYTES;

    /** PROMOTE: op(1) instanceId(4) position(8) proposalTimeNs(8)。 */
    public static final int PROMOTE_LEN = 1 + Integer.BYTES + Long.BYTES * 2;

    /** 响应: status(1) epoch(8) activeInstanceId(4) leaseExpiryNs(8)。 */
    public static final int RESP_LEN = 1 + Long.BYTES + Integer.BYTES + Long.BYTES;

    private HaArbitration() {}

    public static ByteBuffer encodeRegister(int instanceId, byte role, long position) {
        ByteBuffer buf = ByteBuffer.allocate(REGISTER_LEN).order(ByteOrder.BIG_ENDIAN);
        buf.put(OP_REGISTER).put(role).putInt(instanceId).putLong(position);
        buf.flip();
        return buf;
    }

    public static ByteBuffer encodeRenew(
            int instanceId, long epoch, long seq, long nowNs, long ttlMs, long position) {
        ByteBuffer buf = ByteBuffer.allocate(RENEW_LEN).order(ByteOrder.BIG_ENDIAN);
        buf.put(OP_RENEW)
                .putInt(instanceId)
                .putLong(epoch)
                .putLong(seq)
                .putLong(nowNs)
                .putInt((int) ttlMs)
                .putLong(position);
        buf.flip();
        return buf;
    }

    public static ByteBuffer encodePromote(int instanceId, long position, long proposalTimeNs) {
        ByteBuffer buf = ByteBuffer.allocate(PROMOTE_LEN).order(ByteOrder.BIG_ENDIAN);
        buf.put(OP_PROMOTE).putInt(instanceId).putLong(position).putLong(proposalTimeNs);
        buf.flip();
        return buf;
    }

    /** 受控切换请求（布局与 PROMOTE 相同：instanceId + position + nowNs）。 */
    public static ByteBuffer encodeSwitch(int instanceId, long position, long nowNs) {
        ByteBuffer buf = ByteBuffer.allocate(PROMOTE_LEN).order(ByteOrder.BIG_ENDIAN);
        buf.put(OP_SWITCH).putInt(instanceId).putLong(position).putLong(nowNs);
        buf.flip();
        return buf;
    }

    /** 只读查询请求（任意占位内容，状态机忽略）。 */
    public static ByteBuffer encodeQuery() {
        ByteBuffer buf = ByteBuffer.allocate(1).order(ByteOrder.BIG_ENDIAN);
        buf.put(OP_QUERY);
        buf.flip();
        return buf;
    }

    public static ByteBuffer encodeResponse(
            byte status, long epoch, int activeInstanceId, long leaseExpiryNs) {
        ByteBuffer buf = ByteBuffer.allocate(RESP_LEN).order(ByteOrder.BIG_ENDIAN);
        buf.put(status).putLong(epoch).putInt(activeInstanceId).putLong(leaseExpiryNs);
        buf.flip();
        return buf;
    }

    public static Response decodeResponse(ByteString content) {
        ByteBuffer buf = ByteBuffer.wrap(content.toByteArray()).order(ByteOrder.BIG_ENDIAN);
        if (buf.remaining() < RESP_LEN) {
            return new Response(ST_BAD_REQUEST, -1L, 0, 0L);
        }
        return new Response(buf.get(), buf.getLong(), buf.getInt(), buf.getLong());
    }

    /** 请求命令解析出的统一视图。 */
    public record Command(
            byte op,
            byte role,
            int instanceId,
            long epoch,
            long seq,
            long nowNs,
            long ttlMs,
            long position,
            long proposalTimeNs) {

        public static Command parse(byte[] data) {
            ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
            if (buf.remaining() < 1) {
                return new Command(ST_BAD_REQUEST, (byte) 0, 0, 0L, 0L, 0L, 0L, 0L, 0L);
            }
            byte op = buf.get();
            switch (op) {
                case OP_REGISTER:
                    if (buf.remaining() < REGISTER_LEN - 1) {
                        break;
                    }
                    return new Command(
                            op, buf.get(), buf.getInt(), 0L, 0L, 0L, 0L, buf.getLong(), 0L);
                case OP_RENEW:
                    if (buf.remaining() < RENEW_LEN - 1) {
                        break;
                    }
                    return new Command(
                            op,
                            (byte) 0,
                            buf.getInt(),
                            buf.getLong(),
                            buf.getLong(),
                            buf.getLong(),
                            buf.getInt() & 0xFFFFFFFFL,
                            buf.getLong(),
                            0L);
                case OP_PROMOTE:
                    if (buf.remaining() < PROMOTE_LEN - 1) {
                        break;
                    }
                    return new Command(
                            op,
                            (byte) 0,
                            buf.getInt(),
                            0L,
                            0L,
                            0L,
                            0L,
                            buf.getLong(),
                            buf.getLong());
                case OP_SWITCH:
                    if (buf.remaining() < PROMOTE_LEN - 1) {
                        break;
                    }
                    return new Command(
                            op,
                            (byte) 0,
                            buf.getInt(),
                            0L,
                            0L,
                            0L,
                            0L,
                            buf.getLong(),
                            buf.getLong());
                default:
                    break;
            }
            return new Command(ST_BAD_REQUEST, (byte) 0, 0, 0L, 0L, 0L, 0L, 0L, 0L);
        }
    }

    /** 状态机响应（含当前完整仲裁状态，供协调者缓存/比较）。 */
    public record Response(byte status, long epoch, int activeInstanceId, long leaseExpiryNs) {
        public Response {
            Objects.requireNonNull(leaseExpiryNs, "leaseExpiryNs");
        }
    }
}
