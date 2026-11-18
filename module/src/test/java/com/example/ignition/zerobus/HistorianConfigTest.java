package com.example.ignition.zerobus;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class HistorianConfigTest {

    private static boolean startupDelayError(ConfigModel config) {
        return config.validate().stream().anyMatch(e -> e.contains("Startup delay"));
    }

    @Test
    public void startupDelayDefaultsToZeroAndIsValidated() {
        ConfigModel config = new ConfigModel();
        assertEquals(0L, config.getStartupDelayMs());
        assertFalse(startupDelayError(config));
        config.setStartupDelayMs(-1L);
        assertTrue(startupDelayError(config));
        config.setStartupDelayMs(3_600_001L);
        assertTrue(startupDelayError(config));
        config.setStartupDelayMs(30_000L);
        assertFalse(startupDelayError(config));
    }

    @Test
    public void startupDelayChangeIsAppliedAndRestartsCollection() {
        ConfigModel config = new ConfigModel();
        ConfigModel updated = new ConfigModel();
        updated.updateFrom(config);
        updated.setStartupDelayMs(60_000L);
        assertTrue(config.requiresRestart(updated));
        config.updateFrom(updated);
        assertEquals(60_000L, config.getStartupDelayMs());
    }

    @Test
    public void rootFolderIsIncludedInNamesByDefault() {
        ConfigModel config = new ConfigModel();
        assertTrue(config.isIncludeRootFolderInNames());
        ConfigModel updated = new ConfigModel();
        updated.updateFrom(config);
        updated.setIncludeRootFolderInNames(false);
        assertTrue(config.requiresRestart(updated));
        config.updateFrom(updated);
        assertFalse(config.isIncludeRootFolderInNames());
    }

    @Test
    public void logAllSelectedTagsIsOffByDefaultAndRestartsCollection() {
        ConfigModel config = new ConfigModel();
        assertFalse(config.isLogAllSelectedTags());
        ConfigModel updated = new ConfigModel();
        updated.updateFrom(config);
        updated.setLogAllSelectedTags(true);
        assertTrue(config.requiresRestart(updated));
        config.updateFrom(updated);
        assertTrue(config.isLogAllSelectedTags());
    }

    @Test
    public void opcItemPathPropertyDefaultsToBlankAndRestartsCollection() {
        ConfigModel config = new ConfigModel();
        assertEquals("", config.getOpcItemPathProperty());
        ConfigModel updated = new ConfigModel();
        updated.updateFrom(config);
        updated.setOpcItemPathProperty("  cp-opcItemPath ");
        assertEquals("cp-opcItemPath", updated.getOpcItemPathProperty());
        assertTrue(config.requiresRestart(updated));
        config.updateFrom(updated);
        assertEquals("cp-opcItemPath", config.getOpcItemPathProperty());
        updated.setOpcItemPathProperty(null);
        assertEquals("", updated.getOpcItemPathProperty());
    }
}
