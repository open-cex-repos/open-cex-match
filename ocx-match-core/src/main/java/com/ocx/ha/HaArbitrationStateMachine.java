package com.ocx.ha;

import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 仲裁组状态机（2026-09-01）：只维护 {@code epoch}/{@code activeInstanceId}/
 * {@code leaseExpiryNs} + 实例成员表。所有状态变更经 Raft 日志复制，三节点
 * apply 完全相同的命令；时钟值由提议方（leader 协调节点）注入请求负载，
 * 因此租约判定是确定性的。
 *
 * <p>安全不变量：</p>
 * <ul>
 *   <li>PROMOTE 仅当 {@code proposalTimeNs >= leaseExpiryNs}（租约在提议方时钟下
 *       已过期）才提交，提交即 {@code epoch++}——旧 active 的 RENEW 将得到
 *       {@link HaArbitration#ST_EPOCH_MISMATCH}，配合引擎侧 self-fencing 吊销；</li>
 *   <li>提升后新 active 立刻获得新租约，杜绝同轮重复提升（第二个 PROMOTE 的
 *       proposalTimeNs 必然 &lt; 新 leaseExpiryNs，被 {@link HaArbitration#ST_LEASE_ACTIVE} 拒绝）；</li>
 *   <li>REGISTER 幂等；首个 primary 注册即初始 active（epoch=1），与既有
 *       HaCoordinatorMain 语义一致。</li>
 * </ul>
 */
public final class HaArbitrationStateMachine extends BaseStateMachine {

    private final long ttlNs;

    /** 仲裁状态由 Raft 线程写、metrics HTTP 线程读：字段 volatile + 成员表并发安全。 */
    private volatile long epoch;

    private volatile int activeInstanceId;
    private volatile long leaseExpiryNs;
    private final Map<Integer, Member> members = new ConcurrentHashMap<>();

    public HaArbitrationStateMachine(long ttlMs) {
        this.ttlNs = ttlMs * 1_000_000L;
    }

    public long epoch() {
        return epoch;
    }

    public int activeInstanceId() {
        return activeInstanceId;
    }

    public long leaseExpiryNs() {
        return leaseExpiryNs;
    }

    /** 实例复制位置（备机追平进度，提升候选排序用）；未注册返回 -1。 */
    public long position(int instanceId) {
        Member member = members.get(instanceId);
        return member == null ? -1L : member.position;
    }

    /** 实例是否已注册。 */
    public boolean isMember(int instanceId) {
        return members.containsKey(instanceId);
    }

    /** 实例角色；未注册返回 0。 */
    public byte role(int instanceId) {
        Member member = members.get(instanceId);
        return member == null ? 0 : member.role;
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        final LogEntryProto entry = Objects.requireNonNull(trx.getLogEntry());
        final byte[] request = entry.getStateMachineLogEntry().getLogData().toByteArray();
        final byte[] response = apply(HaArbitration.Command.parse(request));
        updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
        return CompletableFuture.completedFuture(Message.valueOf(ByteString.copyFrom(response)));
    }

    @Override
    public CompletableFuture<Message> query(Message request) {
        return CompletableFuture.completedFuture(
                Message.valueOf(
                        ByteString.copyFrom(
                                HaArbitration.encodeResponse(
                                        HaArbitration.ST_OK,
                                        epoch,
                                        activeInstanceId,
                                        leaseExpiryNs))));
    }

    /** 纯函数式 apply：同一命令在任何节点产生同一状态/响应。 */
    private byte[] apply(HaArbitration.Command cmd) {
        switch (cmd.op()) {
            case HaArbitration.OP_REGISTER:
                return register(cmd);
            case HaArbitration.OP_RENEW:
                return renew(cmd);
            case HaArbitration.OP_PROMOTE:
                return promote(cmd);
            case HaArbitration.OP_SWITCH:
                return switchActive(cmd);
            default:
                return response(HaArbitration.ST_BAD_REQUEST);
        }
    }

    private byte[] register(HaArbitration.Command cmd) {
        Member existing = members.get(cmd.instanceId());
        if (existing == null) {
            members.put(cmd.instanceId(), new Member(cmd.role(), cmd.position()));
        } else {
            // 幂等更新角色/复制位置（位置可反映备机追平进度）
            existing.role = cmd.role();
            existing.position = Math.max(existing.position, cmd.position());
        }
        if (epoch == 0L && activeInstanceId == 0 && cmd.role() == HaArbitration.ROLE_PRIMARY) {
            epoch = 1L;
            activeInstanceId = cmd.instanceId();
            leaseExpiryNs = 0L; // 等待首次 RENEW 授予租约
        }
        return response(HaArbitration.ST_OK);
    }

    private byte[] renew(HaArbitration.Command cmd) {
        if (!members.containsKey(cmd.instanceId())) {
            return response(HaArbitration.ST_UNKNOWN_INSTANCE);
        }
        // 持续上报复制位置（备机入站消费进度）：成员表 position 实时更新，
        // 提升候选按"追得最齐"选优（max 防止时钟/顺序回退）
        Member member = members.get(cmd.instanceId());
        member.position = Math.max(member.position, cmd.position());
        if (cmd.instanceId() != activeInstanceId) {
            // standby 心跳确认：不改状态，仅返回当前仲裁状态（epoch 变化即被提升信号）
            return response(HaArbitration.ST_OK);
        }
        if (cmd.epoch() != epoch) {
            // 旧 active 续租被吊销：携带新 epoch，引擎侧 self-fencing
            return response(HaArbitration.ST_EPOCH_MISMATCH);
        }
        // seq 不参与拒绝：协调者重启后单调时钟会重置，用 seq 拒旧会误停续租。
        // expiry 取 max：不同协调者时钟域交错时保证租约只延长不倒退（安全侧）。
        leaseExpiryNs = Math.max(leaseExpiryNs, cmd.nowNs() + ttlNs);
        return response(HaArbitration.ST_OK);
    }

    private byte[] promote(HaArbitration.Command cmd) {
        if (!members.containsKey(cmd.instanceId())) {
            return response(HaArbitration.ST_UNKNOWN_INSTANCE);
        }
        if (cmd.instanceId() == activeInstanceId) {
            return response(HaArbitration.ST_ALREADY_ACTIVE);
        }
        if (cmd.proposalTimeNs() < leaseExpiryNs) {
            return response(HaArbitration.ST_LEASE_ACTIVE);
        }
        epoch++;
        activeInstanceId = cmd.instanceId();
        leaseExpiryNs = cmd.proposalTimeNs() + ttlNs;
        members.get(cmd.instanceId()).position =
                Math.max(members.get(cmd.instanceId()).position, cmd.position());
        return response(HaArbitration.ST_OK);
    }

    /** 受控切换（回切流程）：operator 显式指定新 active，跳过租约过期检查；
     *  提交即 epoch++（旧 active 被吊销）并授予新租约。 */
    private byte[] switchActive(HaArbitration.Command cmd) {
        if (!members.containsKey(cmd.instanceId())) {
            return response(HaArbitration.ST_UNKNOWN_INSTANCE);
        }
        if (cmd.instanceId() == activeInstanceId) {
            return response(HaArbitration.ST_ALREADY_ACTIVE);
        }
        epoch++;
        activeInstanceId = cmd.instanceId();
        leaseExpiryNs = cmd.proposalTimeNs() + ttlNs;
        Member member = members.get(cmd.instanceId());
        member.position = Math.max(member.position, cmd.position());
        return response(HaArbitration.ST_OK);
    }

    private byte[] response(byte status) {
        return HaArbitration.encodeResponse(status, epoch, activeInstanceId, leaseExpiryNs).array();
    }

    static final class Member {
        volatile byte role;
        volatile long position;

        Member(byte role, long position) {
            this.role = role;
            this.position = position;
        }
    }
}
