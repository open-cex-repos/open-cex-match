package com.ocx.aeron;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;

import net.openhft.affinity.Affinity;

import org.agrona.ErrorHandler;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.NoOpIdleStrategy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;

/**
 * 独立媒体驱动进程（三进程形态的共享传输基础设施，先于 ingress/core/egress 启动）。
 * 目录由 {@code -Docx.aeron.dir=} 指定，默认 /tmp/ocx-aeron。
 * <p>
 * 高性能对照开关（默认关闭，保持 Aeron 默认 BackOff 省电配置）：
 * <ul>
 *   <li>{@code -Docx.driver.low-latency=true}：DEDICATED 线程模式 + conductor BusySpin +
 *       sender/receiver/sharedNetwork NoOpIdleStrategy + 稀疏文件关闭（等价官方
 *       LowLatencyMediaDriver 推荐配置）。代价：空闲时也 100% 占核。</li>
 *   <li>{@code -Docx.driver.pin-base=N}：conductor/sender/receiver 分别钉到 N、N+1、N+2 号核
 *       （Linux 生效，macOS 无 sched_setaffinity 为 no-op）；需避开撮合线程已占用的核。</li>
 * </ul>
 * 无论开关如何都显式设置 errorHandler：driver 内部故障（send-channel 失败、资源耗尽等）
 * 不再静默，避免压测异常无法归因到 driver 侧。
 */
public final class OcxMediaDriverMain {

    private static final CountDownLatch LATCH = new CountDownLatch(1);

    private OcxMediaDriverMain() {}

    public static void main(String[] args) throws InterruptedException {
        String dir = System.getProperty("ocx.aeron.dir", "/tmp/ocx-aeron");
        boolean lowLatency =
                Boolean.parseBoolean(System.getProperty("ocx.driver.low-latency", "false"));
        int pinBase = Integer.getInteger("ocx.driver.pin-base", -1);

        MediaDriver.Context context =
                new MediaDriver.Context().aeronDirectoryName(dir).errorHandler(errorHandler());
        if (lowLatency) {
            applyLowLatency(context);
        }
        if (pinBase >= 0) {
            context.conductorThreadFactory(pinnedFactory("Driver-Conductor-", pinBase));
            context.senderThreadFactory(pinnedFactory("Driver-Sender-", pinBase + 1));
            context.receiverThreadFactory(pinnedFactory("Driver-Receiver-", pinBase + 2));
        }

        MediaDriver driver = MediaDriver.launch(context);
        System.err.println(
                "[OcxMediaDriver] MEDIA_DRIVER_READY dir="
                        + dir
                        + " lowLatency="
                        + lowLatency
                        + " pinBase="
                        + pinBase);
        ControlStream.publishReady(dir, "media-driver");
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    driver.close();
                                    LATCH.countDown();
                                },
                                "media-driver-shutdown"));
        LATCH.await();
    }

    private static void applyLowLatency(MediaDriver.Context context) {
        context.threadingMode(ThreadingMode.DEDICATED);
        context.termBufferSparseFile(false);
        context.conductorIdleStrategy(new BusySpinIdleStrategy());
        context.senderIdleStrategy(new NoOpIdleStrategy());
        context.receiverIdleStrategy(new NoOpIdleStrategy());
        context.sharedNetworkIdleStrategy(new NoOpIdleStrategy());
        context.sharedIdleStrategy(new NoOpIdleStrategy());
    }

    private static ErrorHandler errorHandler() {
        return t -> System.err.println("[OcxMediaDriver] ERROR " + t);
    }

    /** 线程启动后立刻把自身钉到指定核；非 Linux 平台 Affinity.setAffinity 为 no-op，静默降级。 */
    private static ThreadFactory pinnedFactory(String prefix, int cpu) {
        return runnable -> {
            Thread thread =
                    new Thread(
                            () -> {
                                try {
                                    Affinity.setAffinity(cpu);
                                } catch (Throwable ignore) {
                                    // 平台不支持时忽略，仅 Linux 服务器上真正生效
                                }
                                runnable.run();
                            },
                            prefix + cpu);
            thread.setDaemon(true);
            return thread;
        };
    }
}
