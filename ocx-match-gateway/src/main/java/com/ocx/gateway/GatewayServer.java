package com.ocx.gateway;

import com.ocx.codec.IoCodec;
import com.ocx.codec.domain.OrderIn;
import com.ocx.math.Decimal;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import io.aeron.Aeron;
import io.aeron.Publication;

import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 订单网关 HTTP 服务（2026-09-01，JDK HttpServer 零框架依赖）：
 * <ul>
 *   <li>{@code POST /orders?clientOrderId=..&symbol=..&side=..&price=..&qty=..}
 *       —— clientOrderId 幂等换发 orderId（重复提交返回原 orderId，不发重复单），
 *       组标准 OrderIn 帧发布到 ingress；</li>
 *   <li>{@code GET /orders/{clientOrderId}} —— 查询映射。</li>
 * </ul>
 * 重复提交（created=false）响应 status=duplicate，客户端据此转状态核对。
 */
public final class GatewayServer {

    private final Aeron aeron;
    private final String ingressEndpoint;
    private final IdMapping idMapping;
    private final Map<String, Publication> publications = new HashMap<>();
    private final AtomicLong acceptedCount = new AtomicLong();
    private final AtomicLong duplicateCount = new AtomicLong();

    public GatewayServer(Aeron aeron, String ingressEndpoint, IdMapping idMapping) {
        this.aeron = aeron;
        this.ingressEndpoint = ingressEndpoint;
        this.idMapping = idMapping;
    }

    public void start(int port) throws IOException {
        HttpServer http = HttpServer.create(new InetSocketAddress(port), 0);
        http.createContext("/orders", this::handleOrders);
        http.start();
        System.err.println("[Gateway] HTTP_READY port=" + port + " ingress=" + ingressEndpoint);
    }

    private void handleOrders(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String path = exchange.getRequestURI().getPath();
        try {
            if ("POST".equals(method) && path.equals("/orders")) {
                Map<String, String> params = parseParams(exchange);
                respond(exchange, place(params));
            } else if ("GET".equals(method) && path.startsWith("/orders/")) {
                String clientOrderId =
                        URLDecoder.decode(
                                path.substring("/orders/".length()), StandardCharsets.UTF_8);
                Long orderId = idMapping.lookup(clientOrderId);
                respond(
                        exchange,
                        orderId == null
                                ? "{\"clientOrderId\":\""
                                        + jsonEscape(clientOrderId)
                                        + "\",\"status\":\"not_found\"}"
                                : "{\"clientOrderId\":\""
                                        + jsonEscape(clientOrderId)
                                        + "\",\"orderId\":"
                                        + orderId
                                        + ",\"status\":\"mapped\"}");
            } else {
                respondCode(exchange, 404, "{\"status\":\"not_found\"}");
            }
        } catch (IllegalArgumentException e) {
            respondCode(
                    exchange,
                    400,
                    "{\"status\":\"bad_request\",\"error\":\""
                            + jsonEscape(e.getMessage())
                            + "\"}");
        }
    }

    private String place(Map<String, String> p) {
        String clientOrderId = require(p, "clientOrderId");
        String symbol = require(p, "symbol");
        IdMapping.MapResult mapped = idMapping.map(clientOrderId);
        if (!mapped.created()) {
            duplicateCount.incrementAndGet();
            return "{\"clientOrderId\":\""
                    + jsonEscape(clientOrderId)
                    + "\",\"orderId\":"
                    + mapped.orderId()
                    + ",\"status\":\"duplicate\"}";
        }

        long userId = Long.parseLong(p.getOrDefault("userId", "0"));
        byte side = Byte.parseByte(require(p, "side"));
        byte type = Byte.parseByte(p.getOrDefault("type", "1"));
        String timeInForce = p.getOrDefault("timeInForce", "GTC");
        if (side != 0 && side != 1) {
            throw new IllegalArgumentException("side 必须为 0(BUY)/1(SELL)");
        }
        if (type < 1 || type > 6) {
            throw new IllegalArgumentException("type 越界(1..6)");
        }
        if (!timeInForce.equals("GTC")
                && !timeInForce.equals("IOC")
                && !timeInForce.equals("FOK")
                && !timeInForce.equals("POST_ONLY")) {
            throw new IllegalArgumentException("timeInForce 必须为 GTC/IOC/FOK/POST_ONLY");
        }
        Decimal price = Decimal.parse(require(p, "price"));
        Decimal qty = Decimal.parse(require(p, "qty"));
        Decimal visibleQty =
                p.containsKey("visibleQty") ? Decimal.parse(p.get("visibleQty")) : null;

        OrderIn order =
                new OrderIn(
                        mapped.orderId(),
                        userId,
                        symbol,
                        price,
                        qty,
                        null,
                        null,
                        side,
                        type,
                        timeInForce,
                        null,
                        LocalDateTime.now(),
                        visibleQty);
        publish(symbol, order);
        acceptedCount.incrementAndGet();
        return "{\"clientOrderId\":\""
                + jsonEscape(clientOrderId)
                + "\",\"orderId\":"
                + mapped.orderId()
                + ",\"status\":\"accepted\"}";
    }

    private void publish(String symbol, OrderIn order) {
        Publication publication =
                publications.computeIfAbsent(
                        symbol,
                        s ->
                                aeron.addPublication(
                                        "aeron:udp?endpoint=" + ingressEndpoint,
                                        InboundStreamIds.inbound(s)));
        byte[] frame = IoCodec.encodeOrderIn(order);
        UnsafeBuffer buffer = new UnsafeBuffer(frame);
        long deadline = System.currentTimeMillis() + 2_000L;
        while (publication.offer(buffer, 0, frame.length) < 0L) {
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("ingress 发布超时 symbol=" + symbol);
            }
            Thread.onSpinWait();
        }
    }

    public long acceptedCount() {
        return acceptedCount.get();
    }

    public long duplicateCount() {
        return duplicateCount.get();
    }

    private static Map<String, String> parseParams(HttpExchange exchange) throws IOException {
        String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        String query = exchange.getRequestURI().getRawQuery();
        Map<String, String> params = new HashMap<>();
        for (String part : (body.isBlank() ? (query == null ? "" : query) : body).split("&")) {
            if (part.isBlank()) {
                continue;
            }
            int eq = part.indexOf('=');
            String key =
                    URLDecoder.decode(
                            eq < 0 ? part : part.substring(0, eq), StandardCharsets.UTF_8);
            String value =
                    eq < 0 ? "" : URLDecoder.decode(part.substring(eq + 1), StandardCharsets.UTF_8);
            params.put(key, value);
        }
        return params;
    }

    private static String require(Map<String, String> p, String key) {
        String v = p.get(key);
        if (v == null || v.isBlank()) {
            throw new IllegalArgumentException("缺少参数: " + key);
        }
        return v;
    }

    private static void respond(HttpExchange exchange, String json) throws IOException {
        respondCode(exchange, 200, json);
    }

    private static void respondCode(HttpExchange exchange, int code, String json)
            throws IOException {
        byte[] body = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(code, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
    }

    private static String jsonEscape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
