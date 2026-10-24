package com.ocx.bench;

import com.ocx.aeron.AeronWire;
import com.ocx.codec.IoCodec;

import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

/**
 * AeronWire 跨进程行编码热路径：core 撮合线程批末 WAL 打包（CREATE/CANCEL/MATCH 子行）
 * 与 ingress 入站行透传（{@code encodeInboundFrame}）。纯缓冲区写，offset 环形滚动。
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
public class AeronWireBench {

    private static final int BUF_SIZE = 1 << 16;
    private static final int MASK = BUF_SIZE - 1;

    private final UnsafeBuffer buf = new UnsafeBuffer(new byte[BUF_SIZE]);
    private final UnsafeBuffer frame =
            new UnsafeBuffer(new byte[IoCodec.MAX_ORDER_IN_FRAME_LENGTH]);
    private int offset;

    /** 回绕到安全写位置：靠近缓冲末尾（放不下整行）时回到 0，避免越界。 */
    private int walPos(int rowLen) {
        int pos = offset & MASK;
        if (pos > BUF_SIZE - rowLen) {
            pos = 0;
        }
        return pos;
    }

    /** 入站行透传（8B 时刻 + 原始 SBE 帧，定长 264B）。 */
    @Benchmark
    public int inboundFrame() {
        AeronWire.encodeInboundFrame(buf, frame, 0, IoCodec.MAX_ORDER_IN_FRAME_LENGTH, 0L);
        return offset;
    }
}
