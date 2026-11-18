package com.example.ignition.zerobus.historian;

import com.example.ignition.zerobus.proto.MetricMetadataEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Tracks the last published metadata snapshot of every producer/provider/path binding.
 *
 * A snapshot is published on first discovery, on any relevant change, on reappearance, and (as an inactive
 * snapshot) once the binding is missing from {@link #MISSED_SCANS_BEFORE_INACTIVE} consecutive complete inventories.
 * Incomplete scans never count as a miss. Revisions increase per binding and are
 * persisted with the snapshots, so they survive restarts and removal/reappearance. The first scan after
 * construction also re-emits every unchanged persisted snapshot, because snapshots are persisted when emitted and
 * those still undelivered at shutdown may have been lost.
 */
public final class MetadataTracker {
    private static final Logger logger = LoggerFactory.getLogger(MetadataTracker.class);

    public static final String QUALITY_SYSTEM_IGNITION = "ignition";
    public static final int MISSED_SCANS_BEFORE_INACTIVE = 2;

    private final String producerId;
    private final HierarchyParser hierarchy;
    private final MetricIdentity.SourceKeyedRule sourceKeyedRule;
    private final TagPathParts nameRoot;
    private final Path stateFile;
    private final LongSupplier clockMs;
    private final Map<String, MetricMetadataEvent> bindings = new LinkedHashMap<>();
    private final Map<String, Integer> missedScans = new HashMap<>();
    private boolean republishPending = true;

    /**
     * @param nameRoot folder whose path is removed from the start of metric names of the tags below it, or null to
     *                 name every tag by its full provider-free path
     */
    public MetadataTracker(String producerId, HierarchyParser hierarchy, MetricIdentity.SourceKeyedRule sourceKeyedRule,
                           TagPathParts nameRoot, Path stateFile, LongSupplier clockMs) {
        this.producerId = producerId;
        this.hierarchy = hierarchy;
        this.sourceKeyedRule = sourceKeyedRule;
        this.nameRoot = nameRoot;
        this.stateFile = stateFile;
        this.clockMs = clockMs;
        load();
    }

    /**
     * Reconciles one scan.
     *
     * @param observed          bindings observed in this scan
     * @param inventoryComplete true only when the scan covered the whole selection without errors; absence from
     *                          an incomplete scan is not counted as a miss
     * @return snapshots to publish, in order
     */
    public synchronized List<MetricMetadataEvent> reconcile(Collection<ObservedTag> observed, boolean inventoryComplete) {
        long now = clockMs.getAsLong();
        List<MetricMetadataEvent> out = new ArrayList<>();
        Map<String, Boolean> seen = new HashMap<>();
        for (ObservedTag tag : observed) {
            String key = key(tag.tag);
            seen.put(key, Boolean.TRUE);
            missedScans.remove(key);
            MetricMetadataEvent snapshot = snapshotOf(tag);
            MetricMetadataEvent prev = bindings.get(key);
            if (prev != null && withoutVersion(prev).equals(snapshot)) {
                if (republishPending) {
                    out.add(prev);
                }
                continue;
            }
            MetricMetadataEvent published = snapshot.toBuilder()
                    .setMetadataObservedAt(now * 1000L)
                    .setMetadataRevision(nextRevision(prev, now))
                    .build();
            bindings.put(key, published);
            out.add(published);
        }
        if (inventoryComplete) {
            for (Map.Entry<String, MetricMetadataEvent> e : bindings.entrySet()) {
                MetricMetadataEvent prev = e.getValue();
                if (prev.getIsActive() && !seen.containsKey(e.getKey())) {
                    int missed = missedScans.merge(e.getKey(), 1, Integer::sum);
                    if (missed < MISSED_SCANS_BEFORE_INACTIVE) {
                        continue;
                    }
                    missedScans.remove(e.getKey());
                    MetricMetadataEvent inactive = prev.toBuilder()
                            .setIsActive(false)
                            .setMetadataObservedAt(now * 1000L)
                            .setMetadataRevision(nextRevision(prev, now))
                            .build();
                    e.setValue(inactive);
                    seen.put(e.getKey(), Boolean.TRUE);
                    out.add(inactive);
                }
            }
        }
        if (republishPending) {
            for (Map.Entry<String, MetricMetadataEvent> e : bindings.entrySet()) {
                if (!seen.containsKey(e.getKey())) {
                    out.add(e.getValue());
                }
            }
            republishPending = false;
        }
        if (!out.isEmpty()) {
            save();
        }
        return out;
    }

    /** @return the current snapshot of an active binding, or null. */
    public synchronized MetricMetadataEvent getActive(TagPathParts tag) {
        MetricMetadataEvent e = bindings.get(key(tag));
        return e != null && e.getIsActive() ? e : null;
    }

    public synchronized int activeCount() {
        int n = 0;
        for (MetricMetadataEvent e : bindings.values()) {
            if (e.getIsActive()) n++;
        }
        return n;
    }

    MetricMetadataEvent snapshotOf(ObservedTag tag) {
        String name = metricName(tag.tag);
        MetricIdentity identity = MetricIdentity.resolve(tag.tag, name, tag.sourceRef, sourceKeyedRule);
        HierarchyParser.Result parsed = hierarchy.parse(name);
        MetricMetadataEvent.Builder b = MetricMetadataEvent.newBuilder()
                .setMetricId(identity.getMetricId())
                .setProducerId(producerId)
                .setTagPath(tag.tag.getPath())
                .setIsActive(true)
                .setDeviceTagName(parsed.deviceTagName)
                .setQualitySystem(QUALITY_SYSTEM_IGNITION)
                .setIdentityMode(identity.getIdentityMode())
                .setIdentityKey(identity.getIdentityKey());
        if (tag.tag.getProvider() != null) b.setTagProvider(tag.tag.getProvider());
        for (HierarchyParser.Level level : parsed.levels) {
            b.addHierarchy(MetricMetadataEvent.HierarchyLevel.newBuilder()
                    .setLevelName(level.name).setLevelValue(level.value));
        }
        if (tag.sourceRef != null && !tag.sourceRef.isBlank()) b.setSourceRef(tag.sourceRef);
        if (tag.dataType != null) b.setDataType(tag.dataType);
        if (tag.historyEnabled != null) b.setHistoryEnabled(tag.historyEnabled);
        if (tag.historyMaxAgeMs != null) b.setHistoryMaxAgeMs(tag.historyMaxAgeMs);
        if (tag.engineeringUnits != null && !tag.engineeringUnits.isEmpty()) b.setEngineeringUnits(tag.engineeringUnits);
        if (tag.description != null && !tag.description.isEmpty()) b.setDescription(tag.description);
        return b.build();
    }

    /** Tag paths are case-insensitive in Ignition, so the root is matched ignoring case. */
    String metricName(TagPathParts tag) {
        String path = tag.getPath();
        if (nameRoot == null || nameRoot.getPath().isEmpty()) {
            return path;
        }
        if (nameRoot.getProvider() != null && !nameRoot.getProvider().equalsIgnoreCase(tag.getProvider())) {
            return path;
        }
        int len = nameRoot.getPath().length();
        if (path.length() > len + 1 && path.charAt(len) == '/' && path.regionMatches(true, 0, nameRoot.getPath(), 0, len)) {
            return path.substring(len + 1);
        }
        return path;
    }

    private static MetricMetadataEvent withoutVersion(MetricMetadataEvent e) {
        return e.toBuilder().clearMetadataObservedAt().clearMetadataRevision().build();
    }

    private static long nextRevision(MetricMetadataEvent prev, long nowMs) {
        return prev == null ? nowMs : Math.max(prev.getMetadataRevision() + 1, nowMs);
    }

    private static String key(TagPathParts tag) {
        return (tag.getProvider() == null ? "" : tag.getProvider()) + '\u0000' + tag.getPath();
    }

    private void load() {
        if (stateFile == null || !Files.exists(stateFile)) {
            return;
        }
        try (InputStream in = Files.newInputStream(stateFile)) {
            MetricMetadataEvent e;
            while ((e = MetricMetadataEvent.parseDelimitedFrom(in)) != null) {
                TagPathParts tag = TagPathParts.parse(e.hasTagProvider() ? "[" + e.getTagProvider() + "]" + e.getTagPath() : e.getTagPath());
                bindings.put(key(tag), e);
            }
            logger.info("Loaded {} metadata binding(s) from {}", bindings.size(), stateFile);
        } catch (IOException ex) {
            logger.warn("Could not read metadata state {}; bindings will be republished", stateFile, ex);
        }
    }

    private void save() {
        if (stateFile == null) {
            return;
        }
        try {
            Files.createDirectories(stateFile.getParent());
            Path tmp = stateFile.resolveSibling(stateFile.getFileName() + ".tmp");
            try (OutputStream out = Files.newOutputStream(tmp)) {
                for (MetricMetadataEvent e : bindings.values()) {
                    e.writeDelimitedTo(out);
                }
            }
            Files.move(tmp, stateFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException ex) {
            logger.warn("Could not write metadata state {}", stateFile, ex);
        }
    }

    /** Metadata observed for one tag during a scan. Null fields were unavailable. */
    public static final class ObservedTag {
        final TagPathParts tag;
        final Boolean historyEnabled;
        final Long historyMaxAgeMs;
        final String sourceRef;
        final String dataType;
        final String engineeringUnits;
        final String description;

        public ObservedTag(TagPathParts tag, Boolean historyEnabled, Long historyMaxAgeMs, String sourceRef,
                           String dataType, String engineeringUnits, String description) {
            this.tag = tag;
            this.historyEnabled = historyEnabled;
            this.historyMaxAgeMs = historyMaxAgeMs;
            this.sourceRef = sourceRef;
            this.dataType = dataType;
            this.engineeringUnits = engineeringUnits;
            this.description = description;
        }
    }
}
