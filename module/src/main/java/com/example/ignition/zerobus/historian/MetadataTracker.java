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
 * snapshot) when a complete inventory no longer contains the binding. Revisions increase per binding and are
 * persisted with the snapshots, so they survive restarts and removal/reappearance.
 */
public final class MetadataTracker {
    private static final Logger logger = LoggerFactory.getLogger(MetadataTracker.class);

    public static final String QUALITY_SYSTEM_IGNITION = "ignition";

    private final String producerId;
    private final HierarchyParser hierarchy;
    private final MetricIdentity.SourceKeyedRule sourceKeyedRule;
    private final Path stateFile;
    private final LongSupplier clockMs;
    private final Map<String, MetricMetadataEvent> bindings = new LinkedHashMap<>();

    public MetadataTracker(String producerId, HierarchyParser hierarchy, MetricIdentity.SourceKeyedRule sourceKeyedRule,
                           Path stateFile, LongSupplier clockMs) {
        this.producerId = producerId;
        this.hierarchy = hierarchy;
        this.sourceKeyedRule = sourceKeyedRule;
        this.stateFile = stateFile;
        this.clockMs = clockMs;
        load();
    }

    /**
     * Reconciles one scan.
     *
     * @param observed          bindings observed in this scan
     * @param inventoryComplete true only when the scan covered the whole selection without errors; bindings
     *                          absent from an incomplete scan are never retired
     * @return snapshots to publish, in order
     */
    public synchronized List<MetricMetadataEvent> reconcile(Collection<ObservedTag> observed, boolean inventoryComplete) {
        long now = clockMs.getAsLong();
        List<MetricMetadataEvent> out = new ArrayList<>();
        Map<String, Boolean> seen = new HashMap<>();
        for (ObservedTag tag : observed) {
            String key = key(tag.tag);
            seen.put(key, Boolean.TRUE);
            MetricMetadataEvent snapshot = snapshotOf(tag);
            MetricMetadataEvent prev = bindings.get(key);
            if (prev != null && withoutVersion(prev).equals(snapshot)) {
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
                    MetricMetadataEvent retired = prev.toBuilder()
                            .setIsActive(false)
                            .setMetadataObservedAt(now * 1000L)
                            .setMetadataRevision(nextRevision(prev, now))
                            .build();
                    e.setValue(retired);
                    out.add(retired);
                }
            }
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
        MetricIdentity identity = MetricIdentity.resolve(tag.tag, tag.sourceRef, sourceKeyedRule);
        HierarchyParser.Result parsed = hierarchy.parse(tag.tag.getPath());
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
