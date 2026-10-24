package com.ocx.outbound;

/**
 * 死信通道（DLT）：异常/脏数据帧的持久化兜底。
 * 撮合链路已完全脱离 Kafka，DLT 落地为 Aeron Archive 死信流。
 */
public interface DeadLetterSink {

    void send(byte[] payload);
}
