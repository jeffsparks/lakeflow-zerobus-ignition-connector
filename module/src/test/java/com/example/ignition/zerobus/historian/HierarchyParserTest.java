package com.example.ignition.zerobus.historian;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class HierarchyParserTest {

    private static final HierarchyParser FIVE =
            new HierarchyParser(List.of("site", "area", "work_center", "work_unit", "device"));

    @Test
    public void deepPathKeepsRemainingSegmentsInDeviceTagName() {
        HierarchyParser.Result r =
                FIVE.parse("site/area/work_center/work_unit/device/status/admin_tags/control/raw/enable");
        assertEquals(5, r.levels.size());
        assertEquals("device", r.levels.get(4).name);
        assertEquals("device", r.levels.get(4).value);
        assertEquals("status/admin_tags/control/raw/enable", r.deviceTagName);
    }

    @Test
    public void leafIsNeverAHierarchyLevel() {
        HierarchyParser.Result r = FIVE.parse("site/area/work_center/commodity");
        assertEquals(3, r.levels.size());
        assertEquals("work_center", r.levels.get(2).name);
        assertEquals("commodity", r.deviceTagName);
    }

    @Test
    public void exactlyLabelsPlusLeaf() {
        HierarchyParser.Result r = FIVE.parse("s/a/wc/wu/d/tag");
        assertEquals(5, r.levels.size());
        assertEquals("tag", r.deviceTagName);
    }

    @Test
    public void singleSegmentAndNoLabels() {
        assertEquals(0, FIVE.parse("tag").levels.size());
        assertEquals("tag", FIVE.parse("tag").deviceTagName);

        HierarchyParser none = new HierarchyParser(List.of());
        HierarchyParser.Result r = none.parse("a/b/c");
        assertTrue(r.levels.isEmpty());
        assertEquals("a/b/c", r.deviceTagName);
    }

    @Test
    public void levelOrderFollowsLabels() {
        HierarchyParser p = new HierarchyParser(List.of("building", "floor"));
        HierarchyParser.Result r = p.parse("B1/F2/zone/temp");
        assertEquals("building", r.levels.get(0).name);
        assertEquals("B1", r.levels.get(0).value);
        assertEquals("floor", r.levels.get(1).name);
        assertEquals("F2", r.levels.get(1).value);
        assertEquals("zone/temp", r.deviceTagName);
    }
}
