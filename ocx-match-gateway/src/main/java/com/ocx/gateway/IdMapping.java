package com.ocx.gateway;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

/**
 * clientOrderId → orderId 幂等映射（2026-09-01，客户端恢复协议前置）：
 * 同一 clientOrderId 永远映射到同一 orderId（重试安全）；映射 append-only 持久化，
 * 网关重启后恢复（幂等键不丢）。
 */
public final class IdMapping {

    private final OrderIdAllocator allocator;
    private final Path mappingFile;
    private final Map<String, Long> clientToOrderId = new HashMap<>();
    private final Map<Long, String> orderIdToClient = new HashMap<>();

    public IdMapping(OrderIdAllocator allocator, Path mappingFile) {
        this.allocator = allocator;
        this.mappingFile = mappingFile;
        load();
    }

    public record MapResult(long orderId, boolean created) {}

    /** 幂等映射：存在返回原 orderId（created=false，调用方不得重复发单）；否则发号并持久化。 */
    public synchronized MapResult map(String clientOrderId) {
        Long existing = clientToOrderId.get(clientOrderId);
        if (existing != null) {
            return new MapResult(existing, false);
        }
        long orderId = allocator.allocate();
        clientToOrderId.put(clientOrderId, orderId);
        orderIdToClient.put(orderId, clientOrderId);
        append(clientOrderId, orderId);
        return new MapResult(orderId, true);
    }

    public synchronized Long lookup(String clientOrderId) {
        return clientToOrderId.get(clientOrderId);
    }

    public synchronized String clientOf(long orderId) {
        return orderIdToClient.get(orderId);
    }

    private void load() {
        if (mappingFile == null || !Files.exists(mappingFile)) {
            return;
        }
        try {
            for (String line : Files.readAllLines(mappingFile, StandardCharsets.UTF_8)) {
                if (line.isBlank()) {
                    continue;
                }
                int comma = line.indexOf(',');
                if (comma <= 0) {
                    continue;
                }
                String clientOrderId = line.substring(0, comma);
                long orderId = Long.parseLong(line.substring(comma + 1).trim());
                clientToOrderId.put(clientOrderId, orderId);
                orderIdToClient.put(orderId, clientOrderId);
            }
        } catch (IOException | NumberFormatException ignored) {
            // 映射文件损坏：跳过坏行（生产应告警）
        }
    }

    private void append(String clientOrderId, long orderId) {
        if (mappingFile == null) {
            return;
        }
        try {
            Files.createDirectories(mappingFile.getParent());
            Files.writeString(
                    mappingFile,
                    clientOrderId + "," + orderId + "\n",
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND,
                    StandardOpenOption.WRITE);
        } catch (IOException ignored) {
            // 映射持久化失败不阻塞下单（生产应告警）；重启后该 clientOrderId 幂等键丢失
        }
    }
}
