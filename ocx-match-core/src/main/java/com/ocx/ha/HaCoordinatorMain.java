package com.ocx.ha;

import com.ocx.aeron.ControlStream;

import io.aeron.Aeron;
import io.aeron.Subscription;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import java.util.HashMap;
import java.util.Map;

/**
 * 主备协调者（2026-09-01 阶段 B + fencing）：订阅 HA 控制流收集主备心跳（续租），
 * 每收到心跳即回复 LEASE_ACK（携带当前 epoch）；主机心跳超时（租约过期）后
 * {@code epoch++} 并向备机发布 PROMOTE（携带新 epoch）。
 *
 * <p>防双主闭环：提升即换 epoch；主机若还活着，下一次续租 ACK 会带上新 epoch，
 * 主机侧 {@link HaHeartbeat} 检测到 epoch 被吊销 → self-fencing 自杀。</p>
 *
 * <p>参数：{@code -Docx.aeron.dir}（协调者本地媒体驱动）、
 * {@code -Docx.ha.udp.listen}（默认 0.0.0.0:53000）、
 * {@code -Docx.ha.udp.endpoint}（默认 localhost:53000，发布 PROMOTE）、
 * {@code -Docx.ha.primary.ack.endpoint} / {@code -Docx.ha.standby.ack.endpoint}
 * （ACK 目标；同机双实例实验需错开端口）、{@code -Docx.ha.primary-id}（默认 1）、
 * {@code -Docx.ha.standby-id}（默认 2）、{@code -Docx.ha.primary-timeout-ms}
 * （默认 5000）。输出行：{@code PRIMARY_HEARTBEAT_LOST} / {@code PROMOTE_SENT} /
 * {@code EPOCH=}（编排脚本可 grep）。</p>
 */
public final class HaCoordinatorMain {

    private HaCoordinatorMain() {}

    public static void main(String[] args) throws InterruptedException {
        String aeronDir = System.getProperty("ocx.aeron.dir", "/tmp/ocx-aeron");
        String listen = System.getProperty("ocx.ha.udp.listen", "aeron:udp?endpoint=0.0.0.0:53000");
        String publishChannel =
                System.getProperty("ocx.ha.udp.endpoint", "aeron:udp?endpoint=localhost:53000");
        String primaryAckEndpoint =
                System.getProperty(
                        "ocx.ha.primary.ack.endpoint", "aeron:udp?endpoint=localhost:53000");
        String standbyAckEndpoint =
                System.getProperty(
                        "ocx.ha.standby.ack.endpoint", "aeron:udp?endpoint=localhost:53000");
        int primaryId = Integer.getInteger("ocx.ha.primary-id", 1);
        int standbyId = Integer.getInteger("ocx.ha.standby-id", 2);
        long timeoutMs = Long.getLong("ocx.ha.primary-timeout-ms", 5_000L);

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
                Subscription sub =
                        aeron.addSubscription(listen, HaControlStream.HA_CONTROL_STREAM)) {

            Map<Integer, Long> lastHeartbeat = new HashMap<>();
            boolean[] promoted = {false};
            long[] lastPrimaryBeat = {-1L};
            long[] epoch = {1L};

            ControlStream.publishReady(aeronDir, "coordinator");
            System.err.println(
                    "[HaCoordinator] COORDINATOR_READY primary="
                            + primaryId
                            + " standby="
                            + standbyId
                            + " timeoutMs="
                            + timeoutMs
                            + " epoch=1");

            IdleStrategy idle = new BackoffIdleStrategy(100, 100, 1_000, 1_000_000);
            while (true) {
                int work =
                        sub.poll(
                                (buffer, offset, length, header) -> {
                                    if (length < HaControlStream.ROW_LEN) {
                                        return;
                                    }
                                    int type = buffer.getInt(offset);
                                    int id = buffer.getInt(offset + Integer.BYTES);
                                    if (type == HaControlStream.TYPE_HEARTBEAT) {
                                        long now = System.currentTimeMillis();
                                        lastHeartbeat.put(id, now);
                                        if (id == primaryId) {
                                            lastPrimaryBeat[0] = now;
                                        }
                                        // 续租确认：携带当前 epoch（主机侧据此检测吊销）
                                        String ackChannel =
                                                id == primaryId
                                                        ? primaryAckEndpoint
                                                        : id == standbyId
                                                                ? standbyAckEndpoint
                                                                : publishChannel;
                                        HaControlStream.publish(
                                                aeron,
                                                ackChannel,
                                                HaControlStream.TYPE_LEASE_ACK,
                                                id,
                                                "ack",
                                                epoch[0],
                                                500L);
                                    } else if (type == HaControlStream.TYPE_LEASE_ACK) {
                                        // 协调者侧不消费 ACK（预留观测）
                                    }
                                },
                                64);

                if (!promoted[0]) {
                    long now = System.currentTimeMillis();
                    if (lastPrimaryBeat[0] > 0L && now - lastPrimaryBeat[0] > timeoutMs) {
                        promoted[0] = true;
                        epoch[0]++;
                        boolean ok =
                                HaControlStream.publish(
                                        aeron,
                                        publishChannel,
                                        HaControlStream.TYPE_PROMOTE,
                                        standbyId,
                                        "promote",
                                        epoch[0],
                                        3_000L);
                        System.err.println(
                                "[HaCoordinator] PRIMARY_HEARTBEAT_LOST"
                                        + " last="
                                        + lastPrimaryBeat[0]
                                        + " lostMs="
                                        + (now - lastPrimaryBeat[0]));
                        System.err.println(
                                "[HaCoordinator] PROMOTE_SENT standby="
                                        + standbyId
                                        + " EPOCH="
                                        + epoch[0]
                                        + " publish="
                                        + ok);
                    }
                }
                idle.idle(work);
            }
        }
    }
}
