package com.example.ignition.zerobus.historian;

import com.example.ignition.zerobus.proto.CollectorEvent;
import com.example.ignition.zerobus.proto.MetricReading;
import com.example.ignition.zerobus.proto.SpoolRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class HistorianBufferTest {

    @TempDir
    Path tempDir;

    private static SpoolRecord reading(String id) {
        return SpoolRecord.newBuilder().setReading(MetricReading.newBuilder().setMetricId(id).setTime(1L).setQuality(192L)).build();
    }

    @Test
    public void diskBufferReplaysUntilCommittedAndSurvivesRestart() throws Exception {
        HistorianBuffer buf = HistorianBuffer.onDisk(tempDir, 10L * 1024 * 1024, 1000, 1024 * 1024);
        assertTrue(buf.offer(reading("a")));
        assertTrue(buf.offer(SpoolRecord.newBuilder().setCollector(CollectorEvent.newBuilder().setEventType("startup")).build()));

        HistorianBuffer.Drain d1 = buf.drain(10);
        assertEquals(2, d1.records.size());
        assertEquals("a", d1.records.get(0).getReading().getMetricId());
        assertEquals("startup", d1.records.get(1).getCollector().getEventType());
        assertEquals(2, buf.drain(10).records.size(), "uncommitted records are replayed");

        HistorianBuffer reopened = HistorianBuffer.onDisk(tempDir, 10L * 1024 * 1024, 1000, 1024 * 1024);
        HistorianBuffer.Drain d2 = reopened.drain(1);
        assertEquals(1, d2.records.size());
        reopened.commit(d2);
        HistorianBuffer.Drain d3 = reopened.drain(10);
        assertEquals(1, d3.records.size());
        assertTrue(d3.records.get(0).hasCollector());
        reopened.commit(d3);
        assertTrue(reopened.drain(10).isEmpty());
    }

    @Test
    public void fullBuffersRejectAndCount() throws Exception {
        HistorianBuffer mem = HistorianBuffer.inMemory(1);
        assertTrue(mem.offer(reading("a")));
        assertFalse(mem.offer(reading("b")));
        assertEquals(1, mem.getRejected());

        HistorianBuffer disk = HistorianBuffer.onDisk(tempDir, 64, 1000, 1024);
        int accepted = 0;
        while (disk.offer(reading("x"))) {
            accepted++;
        }
        assertTrue(accepted > 0);
        assertEquals(1, disk.getRejected());
    }

    @Test
    public void memoryBufferCommitRemovesOnlyDrainedRecords() {
        HistorianBuffer mem = HistorianBuffer.inMemory(10);
        mem.offer(reading("a"));
        mem.offer(reading("b"));
        HistorianBuffer.Drain d = mem.drain(1);
        mem.offer(reading("c"));
        mem.commit(d);
        HistorianBuffer.Drain rest = mem.drain(10);
        assertEquals(2, rest.records.size());
        assertEquals("b", rest.records.get(0).getReading().getMetricId());
    }
}
