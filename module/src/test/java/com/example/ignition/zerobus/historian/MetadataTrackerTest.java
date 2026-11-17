package com.example.ignition.zerobus.historian;

import com.example.ignition.zerobus.historian.MetadataTracker.ObservedTag;
import com.example.ignition.zerobus.proto.MetricMetadataEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

public class MetadataTrackerTest {

    @TempDir
    Path tempDir;

    private final AtomicLong clock = new AtomicLong(1_000L);

    private MetadataTracker tracker() {
        return new MetadataTracker("gw1",
                new HierarchyParser(List.of("site", "area", "work_center", "work_unit", "device")),
                MetricIdentity.SourceKeyedRule.compile(true, "/raw/"),
                tempDir.resolve("state.bin"),
                clock::get);
    }

    private static ObservedTag tag(String fullPath, String source, boolean enabled, long maxAgeMs) {
        return new ObservedTag(TagPathParts.parse(fullPath), enabled, maxAgeMs, source, "Float8", "degC", null);
    }

    @Test
    public void firstDiscoveryPublishesCompleteSnapshot() {
        List<MetricMetadataEvent> out = tracker().reconcile(
                List.of(tag("[default]s/a/wc/wu/dev/raw/temp", "[PLC]T1", true, 60_000L)), true);
        assertEquals(1, out.size());
        MetricMetadataEvent e = out.get(0);
        assertEquals("gw1", e.getProducerId());
        assertEquals("default", e.getTagProvider());
        assertEquals("s/a/wc/wu/dev/raw/temp", e.getTagPath());
        assertTrue(e.getIsActive());
        assertEquals(5, e.getHierarchyCount());
        assertEquals("raw/temp", e.getDeviceTagName());
        assertEquals("source", e.getIdentityMode());
        assertEquals("s/a/wc/wu::[PLC]T1", e.getIdentityKey());
        assertEquals("ignition", e.getQualitySystem());
        assertEquals(60_000L, e.getHistoryMaxAgeMs());
        assertEquals(1_000_000L, e.getMetadataObservedAt());
        assertFalse(e.hasDescription());
    }

    @Test
    public void unchangedObservationIsNotRepublished() {
        MetadataTracker t = tracker();
        t.reconcile(List.of(tag("[p]a/b", null, true, 0L)), true);
        clock.addAndGet(10_000L);
        assertTrue(t.reconcile(List.of(tag("[p]a/b", null, true, 0L)), true).isEmpty());
    }

    @Test
    public void changePublishesHigherRevision() {
        MetadataTracker t = tracker();
        long r1 = t.reconcile(List.of(tag("[p]a/b", null, true, 0L)), true).get(0).getMetadataRevision();
        List<MetricMetadataEvent> out = t.reconcile(List.of(tag("[p]a/b", null, false, 0L)), true);
        assertEquals(1, out.size());
        assertFalse(out.get(0).getHistoryEnabled());
        assertTrue(out.get(0).getMetadataRevision() > r1);
    }

    @Test
    public void removalOnlyRetiresAfterCompleteInventory() {
        MetadataTracker t = tracker();
        MetricMetadataEvent first = t.reconcile(List.of(tag("[p]a/b", null, true, 5000L)), true).get(0);
        assertTrue(t.reconcile(List.of(), false).isEmpty(), "incomplete scan must not retire");

        List<MetricMetadataEvent> out = t.reconcile(List.of(), true);
        assertEquals(1, out.size());
        MetricMetadataEvent retired = out.get(0);
        assertFalse(retired.getIsActive());
        assertEquals(5000L, retired.getHistoryMaxAgeMs(), "last-known settings retained");
        assertEquals(first.getMetricId(), retired.getMetricId());
        assertTrue(retired.getMetadataRevision() > first.getMetadataRevision());
        assertTrue(t.reconcile(List.of(), true).isEmpty(), "already retired");
    }

    @Test
    public void sourceKeyedRenameRetiresOldBindingAndKeepsMetricId() {
        MetadataTracker t = tracker();
        String id = t.reconcile(List.of(tag("[p]s/a/wc/wu/dev1/raw/x", "[PLC]N7:0", true, 0L)), true).get(0).getMetricId();
        List<MetricMetadataEvent> out = t.reconcile(List.of(tag("[p]s/a/wc/wu/dev2/raw/x", "[PLC]N7:0", true, 0L)), true);
        assertEquals(2, out.size());
        assertTrue(out.get(0).getIsActive());
        assertEquals(id, out.get(0).getMetricId());
        assertFalse(out.get(1).getIsActive());
        assertEquals("s/a/wc/wu/dev1/raw/x", out.get(1).getTagPath());
    }

    @Test
    public void revisionsSurviveRestartAndReappearance() {
        MetadataTracker t1 = tracker();
        long r1 = t1.reconcile(List.of(tag("[p]a/b", null, true, 0L)), true).get(0).getMetadataRevision();
        long r2 = t1.reconcile(List.of(), true).get(0).getMetadataRevision();

        MetadataTracker t2 = tracker();
        assertNull(t2.getActive(TagPathParts.parse("[p]a/b")));
        MetricMetadataEvent back = t2.reconcile(List.of(tag("[p]a/b", null, true, 0L)), true).get(0);
        assertTrue(back.getIsActive());
        assertTrue(r1 < r2 && r2 < back.getMetadataRevision());
        assertTrue(t2.reconcile(List.of(tag("[p]a/b", null, true, 0L)), true).isEmpty(), "state restored");
    }
}
