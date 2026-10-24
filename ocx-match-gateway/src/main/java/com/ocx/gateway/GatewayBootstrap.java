package com.ocx.gateway;

import io.aeron.Aeron;

import java.nio.file.Path;

/**
 * 订单网关入口（2026-09-01）：HTTP 收单 + clientOrderId→orderId 幂等换发 +
 * 组标准 OrderIn 帧发往 ingress（引擎链路零改动）。
 *
 * <p>参数：{@code -Docx.gateway.http.port}（默认 8080）、
 * {@code -Docx.gateway.ingress.endpoint}（默认 localhost:11000）、
 * {@code -Docx.gateway.issuer.id}（发号方 ID，1..255，默认 100）、
 * {@code -Docx.gateway.state.dir}（映射/水位持久化目录，默认 /tmp/ocx-gateway-state）、
 * {@code -Docx.aeron.dir}（连接引擎媒体驱动）。</p>
 */
public final class GatewayBootstrap {

    private GatewayBootstrap() {}

    public static void main(String[] args) throws Exception {
        int httpPort = Integer.getInteger("ocx.gateway.http.port", 8080);
        String ingressEndpoint =
                System.getProperty("ocx.gateway.ingress.endpoint", "localhost:11000");
        long issuerId = Long.getLong("ocx.gateway.issuer.id", 100L);
        String stateDir = System.getProperty("ocx.gateway.state.dir", "/tmp/ocx-gateway-state");
        String aeronDir = System.getProperty("ocx.aeron.dir", "/tmp/ocx-aeron");

        Path dir = Path.of(stateDir);
        OrderIdAllocator allocator =
                new OrderIdAllocator(issuerId, dir.resolve("counter.watermark"));
        IdMapping idMapping = new IdMapping(allocator, dir.resolve("id-mapping.csv"));

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir))) {
            GatewayServer server = new GatewayServer(aeron, ingressEndpoint, idMapping);
            server.start(httpPort);
            System.err.println(
                    "[Gateway] GATEWAY_READY issuerId="
                            + issuerId
                            + " counter="
                            + allocator.currentCounter()
                            + " seg="
                            + allocator.segment());
            new java.util.concurrent.CountDownLatch(1).await();
        }
    }
}
