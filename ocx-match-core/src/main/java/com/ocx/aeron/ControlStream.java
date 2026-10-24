package com.ocx.aeron;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;

import org.agrona.concurrent.UnsafeBuffer;

import java.nio.charset.StandardCharsets;

/**
 * 控制流（2026-08-30，引擎 READY / 压测 GO 统一走 Aeron IPC 控制流，替代 .out 文件
 * grep 与 go 文件握手）：{@code [4B type][16B name]} 定长 20B 行，全局单流
 * {@link StreamIds#CONTROL_STREAM}。
 *
 * <p>type={@link #TYPE_READY}(1)：各引擎进程启动完成发布（媒体驱动/Archive/egress/core/
 * ingress/drain）；type={@link #TYPE_GO}(2)：压测编排放行 producer 重放。</p>
 *
 * <p>引擎侧发布尽力而为（短时重试后静默，不阻塞启动；生产无监听者时即 no-op）；
 * 编排侧等待带建连重试（驱动晚起也能等到）。</p>
 */
public final class ControlStream {

    public static final int CONTROL_STREAM = StreamIds.CONTROL_STREAM;
    public static final int TYPE_READY = 1;
    public static final int TYPE_GO = 2;
    public static final int NAME_LEN = 16;
    public static final int ROW_LEN = Integer.BYTES + NAME_LEN;

    private ControlStream() {}

    public static int encode(UnsafeBuffer buf, int type, String name) {
        buf.putInt(0, type);
        buf.putBytes(Integer.BYTES, nameBytes(name));
        return ROW_LEN;
    }

    static byte[] nameBytes(String name) {
        byte[] raw = name.getBytes(StandardCharsets.US_ASCII);
        byte[] n = new byte[NAME_LEN];
        System.arraycopy(raw, 0, n, 0, Math.min(NAME_LEN, raw.length));
        return n;
    }

    private static boolean matchesName(org.agrona.DirectBuffer buffer, int offset, byte[] target) {
        for (int i = 0; i < NAME_LEN; i++) {
            if (buffer.getByte(offset + i) != target[i]) {
                return false;
            }
        }
        return true;
    }

    /** 发布一条控制行（带重试）。 */
    public static boolean publish(Aeron aeron, int type, String name, long timeoutMs) {
        try (Publication publication = aeron.addPublication(OcxAeron.CHANNEL, CONTROL_STREAM)) {
            UnsafeBuffer buf = new UnsafeBuffer(new byte[ROW_LEN]);
            int len = encode(buf, type, name);
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

    /** 引擎进程启动完成信号（尽力而为：连接/发布失败或超时仅静默）。 */
    public static void publishReady(String aeronDir, String name) {
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir))) {
            publish(aeron, TYPE_READY, name, 3_000L);
        } catch (Exception ignored) {
            // 控制面尽力而为：失败不阻塞引擎启动（编排侧有超时兜底）
        }
    }

    /** 等待指定进程 READY：驱动未起时带重试建连；收到返回 true，超时返回 false。 */
    public static boolean waitReady(String aeronDir, String name, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        byte[] target = nameBytes(name);
        boolean[] found = {false};
        try (Aeron aeron = connectWithRetry(aeronDir, deadline)) {
            if (aeron == null) {
                return false;
            }
            try (Subscription sub = aeron.addSubscription(OcxAeron.CHANNEL, CONTROL_STREAM)) {
                while (System.currentTimeMillis() < deadline) {
                    int polled =
                            sub.poll(
                                    (buffer, offset, length, header) -> {
                                        if (length >= ROW_LEN
                                                && buffer.getInt(offset) == TYPE_READY
                                                && matchesName(
                                                        buffer, offset + Integer.BYTES, target)) {
                                            // 命中：通过轮询结果反映（见下方循环退出条件）
                                            found[0] = true;
                                        }
                                    },
                                    64);
                    if (found[0]) {
                        return true;
                    }
                    if (polled == 0) {
                        Thread.sleep(5);
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return false;
    }

    private static Aeron connectWithRetry(String aeronDir, long deadline)
            throws InterruptedException {
        while (System.currentTimeMillis() < deadline) {
            try {
                return Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
            } catch (Exception ignored) {
                Thread.sleep(100);
            }
        }
        return null;
    }
}
