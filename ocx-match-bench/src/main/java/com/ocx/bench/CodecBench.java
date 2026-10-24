package com.ocx.bench;

import com.ocx.codec.IoCodec;
import com.ocx.codec.domain.OrderIn;
import com.ocx.codec.domain.OrderInFields;
import com.ocx.math.Decimal;

import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

/**
 * SBE OrderIn codec 热路径：ingress 透传编码（{@code encodeOrderIn}）与
 * core 撮合线程零分配直读（{@code readOrderIn}，复用 CodecContext + OrderInFields）。
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
public class CodecBench {

    private final IoCodec.CodecContext ctx = IoCodec.context();
    private final byte[] buf = new byte[IoCodec.MAX_ORDER_IN_FRAME_LENGTH];
    private final UnsafeBuffer direct = new UnsafeBuffer(buf);
    private final OrderInFields fields = new OrderInFields();
    private OrderIn orderIn;
    private int encodedLen;

    @Setup(Level.Trial)
    public void setup() {
        orderIn =
                new OrderIn(
                        1L,
                        1L,
                        "BTCUSDT",
                        Decimal.of(7_000_000_000_000L, -8),
                        Decimal.of(1_000_000L, -8),
                        Decimal.of(0L, -8),
                        null,
                        (byte) 0,
                        (byte) 1,
                        "GTC",
                        (byte) 0,
                        LocalDateTime.now(),
                        null);
        encodedLen = IoCodec.encodeOrderIn(orderIn, buf, 0);
    }

    /** 入站 SBE 编码（ingress 透传路径）。 */
    @Benchmark
    public int encodeOrderIn() {
        return IoCodec.encodeOrderIn(orderIn, buf, 0);
    }

    /** 撮合线程直读（零分配，fields 循环复用）。 */
    @Benchmark
    public long decodeOrderIn() {
        IoCodec.readOrderIn(ctx, direct, 0, encodedLen, fields);
        return fields.id;
    }

    /**
     * A/B 对照：旧路径每消息先 {@code decodeBuffer.wrap} 中转再解码——量化
     * 4.9M 裸跑 JFR 里 15.7% 的 {@code UnsafeBuffer.wrap} 开销（现已被消除）。
     */
    @Benchmark
    public long decodeOrderInLegacyWrap() {
        ctx.decodeBuffer.wrap(direct, 0, encodedLen);
        IoCodec.readOrderIn(ctx, direct, 0, encodedLen, fields);
        return fields.id;
    }
}
