package com.ocx.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

class IdMappingTest {

    @TempDir Path tempDir;

    @Test
    void sameClientOrderIdAlwaysMapsToSameOrderId() {
        IdMapping mapping = new IdMapping(new OrderIdAllocator(100L, null), null);
        IdMapping.MapResult first = mapping.map("CUST-0001");
        assertTrue(first.created());
        IdMapping.MapResult second = mapping.map("CUST-0001");
        assertEquals(first.orderId(), second.orderId(), "重复提交必须返回同一 orderId");
        assertFalse(second.created(), "重复提交标记 created=false（调用方不得重复发单）");
    }

    @Test
    void mappingPersistsAcrossRestart() {
        Path mappingFile = tempDir.resolve("id-mapping.csv");
        IdMapping first = new IdMapping(new OrderIdAllocator(100L, null), mappingFile);
        long orderId = first.map("CUST-0002").orderId();

        IdMapping second = new IdMapping(new OrderIdAllocator(100L, null), mappingFile);
        assertEquals(orderId, second.lookup("CUST-0002"), "重启后幂等键映射必须恢复");
        assertEquals("CUST-0002", second.clientOf(orderId), "反向映射必须恢复");
    }

    @Test
    void unknownClientOrderIdReturnsNull() {
        IdMapping mapping = new IdMapping(new OrderIdAllocator(100L, null), null);
        assertNull(mapping.lookup("NOPE"));
        assertNotNull(mapping.map("CUST-0003"));
    }
}
