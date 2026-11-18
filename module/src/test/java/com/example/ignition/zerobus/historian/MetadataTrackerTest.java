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
        return tracker(null);
    }

    private MetadataTracker tracker(String nameRoot) {
        return new MetadataTracker("gw1",
                new HierarchyParser(List.of("site", "area", "work_center", "work_unit", "device")),
                MetricIdentity.SourceKeyedRule.compile(true, "/raw/"),
                nameRoot == null ? null : TagPathParts.parse(nameRoot),
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
    public void excludedRootFolderIsRemovedFromMetricNames() {
        MetadataTracker t = tracker("[MQTT Engine]spb_telemetry");
        MetricMetadataEvent e = t.reconcile(
                List.of(tag("[MQTT Engine]spb_telemetry/s/a/wc/wu/dev/raw/temp", null, true, 0L)), true).get(0);
        assertEquals("spb_telemetry/s/a/wc/wu/dev/raw/temp", e.getTagPath(), "tag_path stays the full path");
        assertEquals("name", e.getIdentityMode());
        assertEquals("s/a/wc/wu/dev/raw/temp", e.getIdentityKey());
        assertEquals(MetricIdentity.sha1Hex("s/a/wc/wu/dev/raw/temp"), e.getMetricId());
        assertEquals("s", e.getHierarchy(0).getLevelValue());
        assertEquals("raw/temp", e.getDeviceTagName());

        MetricMetadataEvent src = t.reconcile(
                List.of(tag("[mqtt engine]SPB_TELEMETRY/s/a/wc/wu/dev/raw/x", "[PLC]T1", true, 0L)), true).get(0);
        assertEquals("s/a/wc/wu::[PLC]T1", src.getIdentityKey(), "root matched ignoring case; source scope below it");
    }

    @Test
    public void rootFolderIsKeptOutsideTheRootAndForSiblingPrefixes() {
        MetadataTracker t = tracker("[MQTT Engine]spb_telemetry");
        assertEquals("other/a/b", t.metricName(TagPathParts.parse("[MQTT Engine]other/a/b")));
        assertEquals("spb_telemetry2/a", t.metricName(TagPathParts.parse("[MQTT Engine]spb_telemetry2/a")));
        assertEquals("spb_telemetry/a", t.metricName(TagPathParts.parse("[default]spb_telemetry/a")));
        assertEquals("a/b", tracker("spb_telemetry").metricName(TagPathParts.parse("[default]spb_telemetry/a/b")));
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
    public void markedInactiveOnlyAfterTwoConsecutiveCompleteScansWithoutIt() {
        MetadataTracker t = tracker();
        MetricMetadataEvent first = t.reconcile(List.of(tag("[p]a/b", null, true, 5000L)), true).get(0);
        assertTrue(t.reconcile(List.of(), true).isEmpty(), "first miss");
        assertTrue(t.reconcile(List.of(), false).isEmpty(), "incomplete scan is not a miss");
        assertNotNull(t.getActive(TagPathParts.parse("[p]a/b")), "still collecting");

        List<MetricMetadataEvent> out = t.reconcile(List.of(), true);
        assertEquals(1, out.size());
        MetricMetadataEvent inactive = out.get(0);
        assertFalse(inactive.getIsActive());
        assertEquals(5000L, inactive.getHistoryMaxAgeMs(), "last-known settings retained");
        assertEquals(first.getMetricId(), inactive.getMetricId());
        assertTrue(inactive.getMetadataRevision() > first.getMetadataRevision());
        assertNull(t.getActive(TagPathParts.parse("[p]a/b")));
        assertTrue(t.reconcile(List.of(), true).isEmpty(), "already inactive");
    }

    @Test
    public void reappearanceResetsMissedScans() {
        MetadataTracker t = tracker();
        t.reconcile(List.of(tag("[p]a/b", null, true, 0L)), true);
        assertTrue(t.reconcile(List.of(), true).isEmpty());
        assertTrue(t.reconcile(List.of(tag("[p]a/b", null, true, 0L)), false).isEmpty(), "seen in an incomplete scan");
        assertTrue(t.reconcile(List.of(), true).isEmpty(), "first miss again");
        assertFalse(t.reconcile(List.of(), true).get(0).getIsActive());
    }

    @Test
    public void sourceKeyedRenameKeepsMetricIdAndMarksOldPathInactive() {
        MetadataTracker t = tracker();
        String id = t.reconcile(List.of(tag("[p]s/a/wc/wu/dev1/raw/x", "[PLC]N7:0", true, 0L)), true).get(0).getMetricId();
        ObservedTag renamed = tag("[p]s/a/wc/wu/dev2/raw/x", "[PLC]N7:0", true, 0L);
        List<MetricMetadataEvent> out = t.reconcile(List.of(renamed), true);
        assertEquals(1, out.size());
        assertTrue(out.get(0).getIsActive());
        assertEquals(id, out.get(0).getMetricId());
        out = t.reconcile(List.of(renamed), true);
        assertEquals(1, out.size());
        assertFalse(out.get(0).getIsActive());
        assertEquals("s/a/wc/wu/dev1/raw/x", out.get(0).getTagPath());
    }

    @Test
    public void revisionsSurviveRestartAndReappearance() {
        MetadataTracker t1 = tracker();
        long r1 = t1.reconcile(List.of(tag("[p]a/b", null, true, 0L)), true).get(0).getMetadataRevision();
        t1.reconcile(List.of(), true);
        long r2 = t1.reconcile(List.of(), true).get(0).getMetadataRevision();

        MetadataTracker t2 = tracker();
        assertNull(t2.getActive(TagPathParts.parse("[p]a/b")));
        MetricMetadataEvent back = t2.reconcile(List.of(tag("[p]a/b", null, true, 0L)), true).get(0);
        assertTrue(back.getIsActive());
        assertTrue(r1 < r2 && r2 < back.getMetadataRevision());
        assertTrue(t2.reconcile(List.of(tag("[p]a/b", null, true, 0L)), true).isEmpty(), "state restored");
    }

    @Test
    public void firstScanAfterRestartRepublishesPersistedSnapshotsUnchanged() {
        MetadataTracker t1 = tracker();
        MetricMetadataEvent a = t1.reconcile(List.of(tag("[p]a/b", null, true, 0L), tag("[p]a/c", null, true, 0L)), true).get(0);
        t1.reconcile(List.of(tag("[p]a/b", null, true, 0L)), true);
        MetricMetadataEvent inactive = t1.reconcile(List.of(tag("[p]a/b", null, true, 0L)), true).get(0);
        assertFalse(inactive.getIsActive());

        clock.addAndGet(10_000L);
        MetadataTracker t2 = tracker();
        List<MetricMetadataEvent> out = t2.reconcile(List.of(tag("[p]a/b", null, true, 0L)), false);
        assertEquals(List.of(a, inactive), out, "same revisions, inactive binding included");
        assertTrue(t2.reconcile(List.of(tag("[p]a/b", null, true, 0L)), false).isEmpty(), "only the first scan");
    }
}
