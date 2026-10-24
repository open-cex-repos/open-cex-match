package com.ocx.snapshot;

/**
 * 快照条件单扁平字段（纵表链路）— 与 {@link SnapshotOrderFields} 同套路，恢复时直接
 * 反挂载到 {@link com.ocx.book.TriggerSlots}（不重走入站校验/撮合，字段已是定点）。
 *
 * <p>数值约定：triggerPrice/price/trailDelta 为 coinScale 定点；origQty 为
 * baseCoinScale 定点（快照编码 exponent 隐含 0，恢复直接使用）。</p>
 */
public record TriggerSnapshotFields(
        long orderId,
        long userId,
        long triggerPrice,
        long price,
        long origQty,
        int direction,
        int triggerType,
        long trailDelta,
        long time) {}
