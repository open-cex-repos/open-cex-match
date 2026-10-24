package com.ocx.handler;

import com.ocx.codec.IoCodec;
import com.ocx.codec.domain.OrderInFields;

import java.io.Serializable;

/**
 * RingBuffer 事件槽：直接承载 SBE OrderIn 帧字节。
 *
 * <p>入站消费线程只做校验 + 路由 + 一次 memcpy（{@link #sbePayload} 槽位
 * 启动时预分配、循环复用，零分配），不再反序列化为 Order/InboundFields；撮合线程
 * 按偏移直读原生字段（{@code IoCodec.readOrderIn}）。</p>
 */
public class OrderEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    /** SBE OrderIn 帧槽位（含魔数 OCX2），启动时预分配，槽位循环复用。 */
    private final byte[] sbePayload = new byte[IoCodec.MAX_ORDER_IN_FRAME_LENGTH];

    /** 帧有效长度（≤ {@link IoCodec#MAX_ORDER_IN_FRAME_LENGTH}）。 */
    private int sbeLength;

    /**
     * 入站字段容器（发布线程解码填充，撮合线程直读）— 槽位复用，零分配。
     * SBE 帧解析从撮合线程移到消费线程，撮合线程不再碰字节解码。
     */
    private final OrderInFields fields = new OrderInFields();

    /** 发布时已按 symbol 路由，撮合线程据此确认归属（不逐单解析 symbol）。 */
    private String symbol;

    /**
     * 事件时间戳（ms）。RingBuffer 槽位循环复用，事件对象在启动时预分配——
     * 因此时间戳必须由翻译器在每次发布时写入，不能是 final 构造时值
     * （否则读到的是槽位创建时刻而非事件发生时刻，LMAX 事件复用惯例）。
     */
    private long timestamp;

    /** 快照采集请求事件（source 为 null）：作为 endOfBatch 采集触发器，不参与撮合。 */
    private transient boolean snapshotRequest;

    public OrderEvent() {
        this.timestamp = 0L;
    }

    public byte[] getSbePayload() {
        return sbePayload;
    }

    public int getSbeLength() {
        return sbeLength;
    }

    public void setSbeLength(int sbeLength) {
        this.sbeLength = sbeLength;
    }

    public OrderInFields getFields() {
        return fields;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public boolean isSnapshotRequest() {
        return snapshotRequest;
    }

    public void setSnapshotRequest(boolean snapshotRequest) {
        this.snapshotRequest = snapshotRequest;
    }
}
