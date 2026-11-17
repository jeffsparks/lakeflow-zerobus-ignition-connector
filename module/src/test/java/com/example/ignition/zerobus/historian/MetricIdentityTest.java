package com.example.ignition.zerobus.historian;

import com.example.ignition.zerobus.historian.MetricIdentity.SourceKeyedRule;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class MetricIdentityTest {

    private static final SourceKeyedRule RAW = SourceKeyedRule.compile(true, "/raw/");

    private static MetricIdentity resolve(String fullPath, String source, SourceKeyedRule rule) {
        return MetricIdentity.resolve(TagPathParts.parse(fullPath), source, rule);
    }

    @Test
    public void metricIdIsSha1OfIdentityKey() {
        assertEquals("a9993e364706816aba3e25717850c26c9cd0d89d", MetricIdentity.sha1Hex("abc"));
        MetricIdentity id = resolve("[default]a/b/c", null, null);
        assertEquals(MetricIdentity.sha1Hex("a/b/c"), id.getMetricId());
    }

    @Test
    public void nameKeyedUsesProviderFreePathExactly() {
        MetricIdentity id = resolve("[default]Site/Area/WC/WU/Pump1/Speed", "ns=1;s=[PLC]Speed", null);
        assertEquals(MetricIdentity.MODE_NAME, id.getIdentityMode());
        assertEquals("Site/Area/WC/WU/Pump1/Speed", id.getIdentityKey());
        assertNotEquals(id, resolve("[default]site/area/wc/wu/pump1/speed", null, null));
    }

    @Test
    public void providerDoesNotParticipate() {
        assertEquals(resolve("[a]x/y", null, null).getMetricId(), resolve("[b]x/y", null, null).getMetricId());
    }

    @Test
    public void sourceKeyedUsesFirstFourSegmentsAndSource() {
        MetricIdentity id = resolve("[default]site/area/wc/wu/device/status/raw/enable", "[PLC1]N7:0", RAW);
        assertEquals(MetricIdentity.MODE_SOURCE, id.getIdentityMode());
        assertEquals("site/area/wc/wu::[PLC1]N7:0", id.getIdentityKey());
    }

    @Test
    public void sourceKeyedRenameOfExcludedSegmentsKeepsIdentity() {
        MetricIdentity a = resolve("[default]site/area/wc/wu/device1/raw/enable", "[PLC1]N7:0", RAW);
        MetricIdentity b = resolve("[default]site/area/wc/wu/device2/raw/folder/renamed", "[PLC1]N7:0", RAW);
        assertEquals(a.getMetricId(), b.getMetricId());
    }

    @Test
    public void sourceKeyedScopeOrSourceChangeIsNewMetric() {
        MetricIdentity a = resolve("[default]site/area/wc/wu/raw/x", "[PLC1]N7:0", RAW);
        assertNotEquals(a, resolve("[default]site/area/wc/wu2/raw/x", "[PLC1]N7:0", RAW));
        assertNotEquals(a, resolve("[default]site/area/wc/wu/raw/x", "[PLC1]N7:1", RAW));
    }

    @Test
    public void shallowSourceKeyedPathUsesAvailableNonLeafSegments() {
        SourceKeyedRule all = SourceKeyedRule.compile(true, ".*");
        assertEquals("site/area/wc::src", resolve("[p]site/area/wc/leaf", "src", all).getIdentityKey());
        assertEquals("::src", resolve("[p]leaf", "src", all).getIdentityKey());
    }

    @Test
    public void missingSourceFallsBackToNameKeyed() {
        MetricIdentity id = resolve("[default]site/area/wc/wu/raw/memtag", null, RAW);
        assertEquals(MetricIdentity.MODE_NAME, id.getIdentityMode());
        assertEquals(MetricIdentity.MODE_NAME, resolve("[default]a/raw/b", "  ", RAW).getIdentityMode());
    }

    @Test
    public void nonMatchingOrDisabledRuleIsNameKeyed() {
        assertEquals(MetricIdentity.MODE_NAME, resolve("[default]a/b/c/d/e", "src", RAW).getIdentityMode());
        SourceKeyedRule disabled = SourceKeyedRule.compile(false, "/raw/");
        assertEquals(MetricIdentity.MODE_NAME, resolve("[default]a/raw/b", "src", disabled).getIdentityMode());
    }

    @Test
    public void regexIsMatchedAgainstFullPathIncludingProvider() {
        SourceKeyedRule providerRule = SourceKeyedRule.compile(true, "^\\[plc\\]");
        assertEquals(MetricIdentity.MODE_SOURCE, resolve("[plc]a/b", "src", providerRule).getIdentityMode());
        assertEquals(MetricIdentity.MODE_NAME, resolve("[other]a/b", "src", providerRule).getIdentityMode());
    }

    @Test
    public void invalidRegexIsReportedAndMatchesNothing() {
        SourceKeyedRule bad = SourceKeyedRule.compile(true, "([unclosed");
        assertNotNull(bad.getError());
        assertFalse(bad.matches("[default]anything"));
        assertNotNull(SourceKeyedRule.compile(true, "").getError());
        assertNull(SourceKeyedRule.compile(false, "([unclosed").getError());
        assertNull(RAW.getError());
    }
}
