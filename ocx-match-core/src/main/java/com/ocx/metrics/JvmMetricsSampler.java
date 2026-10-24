package com.ocx.metrics;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadMXBean;
import java.util.List;

/**
 * 低成本采集当前 JVM 的 heap / non-heap / thread / GC 指标。
 *
 * <p>该类只在 metrics 导出线程中调用，不进入撮合热路径。GC 指标使用
 * {@link GarbageCollectorMXBean} 的累计 collection count/time；如果运行时暴露
 * {@code com.sun.management.GarbageCollectorMXBean}，再补充最近一次 GC pause。</p>
 */
public final class JvmMetricsSampler {

    private final MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
    private final ThreadMXBean threads = ManagementFactory.getThreadMXBean();
    private final List<GarbageCollectorMXBean> gcBeans =
            ManagementFactory.getGarbageCollectorMXBeans();

    public JvmMetrics sample() {
        MemoryUsage heap = memory.getHeapMemoryUsage();
        MemoryUsage nonHeap = memory.getNonHeapMemoryUsage();

        long gcCollections = 0L;
        long gcCollectionTimeMillis = 0L;
        long gcLastPauseMillis = -1L;

        for (GarbageCollectorMXBean bean : gcBeans) {
            long count = bean.getCollectionCount();
            long time = bean.getCollectionTime();
            if (count >= 0) {
                gcCollections += count;
            }
            if (time >= 0) {
                gcCollectionTimeMillis += time;
            }
            if (bean instanceof com.sun.management.GarbageCollectorMXBean sunBean) {
                com.sun.management.GcInfo lastGc = sunBean.getLastGcInfo();
                if (lastGc != null && lastGc.getDuration() >= 0) {
                    gcLastPauseMillis = Math.max(gcLastPauseMillis, lastGc.getDuration());
                }
            }
        }

        return new JvmMetrics(
                heap.getUsed(),
                heap.getCommitted(),
                heap.getMax(),
                nonHeap.getUsed(),
                threads.getThreadCount(),
                threads.getDaemonThreadCount(),
                threads.getPeakThreadCount(),
                gcCollections,
                gcCollectionTimeMillis,
                gcLastPauseMillis,
                ManagementFactory.getRuntimeMXBean().getUptime());
    }
}
