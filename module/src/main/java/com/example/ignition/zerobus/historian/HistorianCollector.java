package com.example.ignition.zerobus.historian;

import com.example.ignition.zerobus.ConfigModel;
import com.example.ignition.zerobus.TagSelectionResolver;
import com.example.ignition.zerobus.proto.CollectorEvent;
import com.example.ignition.zerobus.proto.MetricMetadataEvent;
import com.example.ignition.zerobus.proto.MetricReading;
import com.example.ignition.zerobus.proto.SpoolRecord;
import com.example.ignition.zerobus.saf.DiskSpool;
import com.example.ignition.zerobus.web.TagEventPayload;
import com.inductiveautomation.ignition.common.Dataset;
import com.inductiveautomation.ignition.common.config.BasicProperty;
import com.inductiveautomation.ignition.common.config.Property;
import com.inductiveautomation.ignition.common.document.DocumentElement;
import com.inductiveautomation.ignition.common.model.values.QualifiedValue;
import com.inductiveautomation.ignition.common.tags.config.properties.TagHistoryProps;
import com.inductiveautomation.ignition.common.tags.config.properties.WellKnownTagProps;
import com.inductiveautomation.ignition.common.tags.config.types.OpcTagTypeProperties;
import com.inductiveautomation.ignition.common.tags.model.TagPath;
import com.inductiveautomation.ignition.common.tags.model.event.TagChangeEvent;
import com.inductiveautomation.ignition.common.tags.model.event.TagChangeListener;
import com.inductiveautomation.ignition.common.util.TimeUnits;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import com.inductiveautomation.ignition.gateway.tags.model.GatewayTagManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects historian readings, metadata snapshots, and collector events for the Zerobus sink.
 *
 * Collection is driven by each tag's Ignition history configuration: tags with history enabled produce a reading
 * on every value or quality change, and an unchanged tag whose last value was Good produces a max-age reading of
 * that value (quality Good_Backfill) once its maximum time between samples elapses. With "log all selected tags"
 * enabled, every selected tag is collected as if history were enabled; snapshots still carry the tag's own setting.
 */
public final class HistorianCollector {
    private static final Logger logger = LoggerFactory.getLogger(HistorianCollector.class);

    static final long HEARTBEAT_INTERVAL_MS = 60_000L;
    static final long MIN_MAX_AGE_MS = 1_000L;
    static final int MAX_AGE_QUALITY = 203; // Good_Backfill
    private static final long MAX_AGE_SCAN_MS = 250L;
    private static final long PERSIST_INTERVAL_MS = 200L;
    private static final long FLUSH_TIME_BUDGET_MS = 5_000L;
    private static final int READ_CHUNK_TAGS = 1_000;
    private static final int MAX_BROWSE_NODES = 1_000_000;
    private static final int MAX_PATTERN_MATCHES = 500_000;

    private final GatewayContext context;
    private final ConfigModel config;
    private final String producerId;
    private final String sessionId = UUID.randomUUID().toString();
    private final HistorianBuffer buffer;
    private final HistorianZerobusClient client;
    private final MetadataTracker tracker;
    private final TagSelectionResolver resolver;
    private final ReadingFactory readings = new ReadingFactory(HistorianCollector::toJson);
    private final MetricIdentity.SourceKeyedRule sourceKeyedRule;
    private final List<Property<?>> metadataProps;

    private final Map<String, TagState> tags = new ConcurrentHashMap<>();
    private final Queue<SpoolRecord> pending = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ScheduledExecutorService timers;
    private ScheduledExecutorService scanner;
    private ScheduledExecutorService flusher;

    private final AtomicLong readingsCollected = new AtomicLong();
    private final AtomicLong readingsRejected = new AtomicLong();
    private final AtomicLong readingsUnsupported = new AtomicLong();
    private final AtomicLong maxAgeReadings = new AtomicLong();
    private final AtomicLong metadataEmitted = new AtomicLong();
    private final AtomicLong httpRejected = new AtomicLong();
    private final AtomicLong batchesSent = new AtomicLong();
    private volatile long lastScanMs = 0L;
    private volatile String lastScanSummary = "not run";

    public HistorianCollector(GatewayContext context, ConfigModel config) throws IOException {
        this.context = context;
        this.config = config;
        this.producerId = resolveProducerId(context, config);
        this.sourceKeyedRule = MetricIdentity.SourceKeyedRule.compile(config.isSourceKeyedEnabled(), config.getSourceKeyedTagRegex());
        if (sourceKeyedRule.getError() != null) {
            logger.error("Source-keyed identity is disabled until fixed: {}", sourceKeyedRule.getError());
        }
        this.metadataProps = List.of(
                WellKnownTagProps.HistoryEnabled,
                TagHistoryProps.HistoryMaxAge,
                TagHistoryProps.HistoryMaxAgeUnits,
                opcItemPathProperty(config),
                WellKnownTagProps.DataType,
                WellKnownTagProps.EngUnit,
                WellKnownTagProps.Documentation);
        Path dir = DiskSpool.resolveDir(config.getSpoolDirectory()).resolve("historian");
        this.buffer = config.isEnableStoreAndForward()
                ? HistorianBuffer.onDisk(dir, config.getSpoolMaxBytes(), Math.max(config.getMaxQueueSize(), config.getBatchSize() * 4), config.getSpoolReadMaxBytes())
                : HistorianBuffer.inMemory(config.getMaxQueueSize());
        this.tracker = new MetadataTracker(producerId, new HierarchyParser(config.getHierarchyLevels()), sourceKeyedRule,
                nameRoot(config), dir.resolve("metadata.state"), System::currentTimeMillis);
        this.client = new HistorianZerobusClient(config);
        this.resolver = new TagSelectionResolver(config, MAX_BROWSE_NODES, MAX_PATTERN_MATCHES);
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        logger.info("Starting historian collector (producer_id={}, session_id={}, buffer={}, startup delay={} ms, source reference property={}, log all selected tags={})",
                producerId, sessionId, buffer.isDiskBacked() ? "disk" : "memory", config.getStartupDelayMs(), opcItemPathProperty(config).getName(),
                config.isLogAllSelectedTags());
        timers = Executors.newScheduledThreadPool(2, daemon("Zerobus-Historian-Timer"));
        scanner = Executors.newSingleThreadScheduledExecutor(daemon("Zerobus-Historian-Metadata"));
        flusher = Executors.newSingleThreadScheduledExecutor(daemon("Zerobus-Historian-Flush"));

        enqueue(collectorEvent("startup"));
        timers.scheduleAtFixedRate(() -> enqueue(collectorEvent("heartbeat")),
                HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
        timers.scheduleAtFixedRate(this::emitDueMaxAgeReadings, MAX_AGE_SCAN_MS, MAX_AGE_SCAN_MS, TimeUnit.MILLISECONDS);
        timers.scheduleAtFixedRate(buffer::persistStaged, PERSIST_INTERVAL_MS, PERSIST_INTERVAL_MS, TimeUnit.MILLISECONDS);
        if (!config.isEnableDirectSubscriptions()) {
            logger.info("Direct subscriptions disabled; readings come only from HTTP ingest for scanned bindings");
        }
        scanner.scheduleWithFixedDelay(this::scanMetadata, Math.max(0L, config.getStartupDelayMs()),
                config.getMetadataCheckIntervalMs(), TimeUnit.MILLISECONDS);
        flusher.scheduleWithFixedDelay(this::flush, config.getBatchFlushIntervalMs(), config.getBatchFlushIntervalMs(), TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        logger.info("Shutting down historian collector");
        unsubscribe(new ArrayList<>(tags.values()));
        enqueue(collectorEvent("shutdown"));
        for (ScheduledExecutorService s : new ScheduledExecutorService[] {timers, scanner, flusher}) {
            if (s != null) s.shutdownNow();
        }
        drainPending();
        buffer.persistStaged();
        // Best-effort final send; disk-backed records not sent here are delivered after restart.
        Thread t = new Thread(this::flush, "Zerobus-Historian-FinalFlush");
        t.setDaemon(true);
        t.start();
        try {
            t.join(3_000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        client.shutdown();
    }

    public boolean isRunning() {
        return running.get();
    }

    public boolean testConnection() {
        return client.testConnection();
    }

    // ---------------------------------------------------------------------------------------------------------
    // Metadata
    // ---------------------------------------------------------------------------------------------------------

    void scanMetadata() {
        try {
            GatewayTagManager tm = context.getTagManager();
            TagSelectionResolver.Selection selection = resolver.resolve(tm);
            boolean complete = selection.complete;
            List<MetadataTracker.ObservedTag> observed = new ArrayList<>(selection.paths.size());
            Map<String, TagPath> pathsByKey = new HashMap<>();

            for (int i = 0; i < selection.paths.size(); i += READ_CHUNK_TAGS) {
                List<TagPath> chunk = selection.paths.subList(i, Math.min(i + READ_CHUNK_TAGS, selection.paths.size()));
                List<TagPath> propertyPaths = new ArrayList<>(chunk.size() * metadataProps.size());
                for (TagPath tp : chunk) {
                    for (Property<?> p : metadataProps) {
                        propertyPaths.add(tp.getChildPath(p));
                    }
                }
                List<QualifiedValue> values;
                try {
                    values = tm.readAsync(propertyPaths).get(Math.max(1_000L, config.getRequestTimeoutMs()), TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    logger.warn("Metadata read failed for {} tag(s); missing tags are not counted for this scan: {}", chunk.size(), e.toString());
                    complete = false;
                    continue;
                }
                for (int t = 0; t < chunk.size(); t++) {
                    TagPath tp = chunk.get(t);
                    MetadataTracker.ObservedTag tag = toObserved(tp, values.subList(t * metadataProps.size(), (t + 1) * metadataProps.size()));
                    if (tag != null) {
                        observed.add(tag);
                        pathsByKey.put(tp.toString(), tp);
                    }
                }
            }

            List<MetricMetadataEvent> events = tracker.reconcile(observed, complete);
            for (MetricMetadataEvent e : events) {
                enqueue(SpoolRecord.newBuilder().setMetadata(e).build());
            }
            metadataEmitted.addAndGet(events.size());
            applyCollection(pathsByKey);
            lastScanMs = System.currentTimeMillis();
            lastScanSummary = String.format("%d tag(s) observed, %d snapshot(s) emitted, inventory %s",
                    observed.size(), events.size(), complete ? "complete" : "incomplete");
            logger.info("Metadata scan: {}", lastScanSummary);
        } catch (Throwable t) {
            logger.error("Metadata scan failed", t);
        }
    }

    private MetadataTracker.ObservedTag toObserved(TagPath tp, List<QualifiedValue> v) {
        Object historyEnabled = good(v.get(0));
        if (historyEnabled == null && !isGood(v.get(4))) {
            return null; // tag disappeared between browse and read
        }
        return new MetadataTracker.ObservedTag(
                TagPathParts.parse(tp.toString()),
                historyEnabled == null ? null : Boolean.valueOf(historyEnabled.toString()),
                maxAgeMs(good(v.get(1)), good(v.get(2)), tp),
                asString(good(v.get(3))),
                asString(good(v.get(4))),
                asString(good(v.get(5))),
                asString(good(v.get(6))));
    }

    private static Long maxAgeMs(Object age, Object units, TagPath tp) {
        if (age == null) {
            return null;
        }
        double magnitude;
        try {
            magnitude = Double.parseDouble(age.toString());
        } catch (NumberFormatException e) {
            return null;
        }
        if (magnitude <= 0) {
            return 0L;
        }
        TimeUnits unit = null;
        if (units instanceof TimeUnits) {
            unit = (TimeUnits) units;
        } else if (units != null) {
            try {
                unit = TimeUnits.valueOf(units.toString().trim().toUpperCase());
            } catch (IllegalArgumentException ignored) {
                // reported below
            }
        }
        if (unit == null) {
            logger.warn("Unrecognized historyMaxAgeUnits '{}' on {}; maximum interval unavailable", units, tp);
            return null;
        }
        return TimeUnits.toMillis(magnitude, unit);
    }

    private void applyCollection(Map<String, TagPath> observedPaths) {
        List<TagState> toSubscribe = new ArrayList<>();
        List<TagState> toUnsubscribe = new ArrayList<>();
        for (Map.Entry<String, TagPath> e : observedPaths.entrySet()) {
            MetricMetadataEvent snapshot = tracker.getActive(TagPathParts.parse(e.getKey()));
            if (snapshot == null) {
                continue;
            }
            TagState state = tags.computeIfAbsent(e.getKey(), k -> new TagState(e.getValue()));
            state.metricId = snapshot.getMetricId();
            state.dataType = snapshot.hasDataType() ? snapshot.getDataType() : null;
            state.maxAgeMs = snapshot.hasHistoryMaxAgeMs() ? snapshot.getHistoryMaxAgeMs() : 0L;
            boolean enabled = collects(snapshot);
            if (enabled && !state.subscribed && config.isEnableDirectSubscriptions()) {
                toSubscribe.add(state);
            } else if (!enabled && state.subscribed) {
                toUnsubscribe.add(state);
            }
            state.enabled = enabled;
        }
        // A tag missing from this scan keeps collecting until the tracker marks it inactive.
        for (TagState s : new ArrayList<>(tags.values())) {
            String key = s.path.toString();
            if (!observedPaths.containsKey(key) && tracker.getActive(TagPathParts.parse(key)) == null) {
                s.enabled = false;
                if (s.subscribed) toUnsubscribe.add(s);
                tags.remove(key);
            }
        }
        subscribe(toSubscribe);
        unsubscribe(toUnsubscribe);
    }

    private void subscribe(List<TagState> states) {
        if (states.isEmpty()) {
            return;
        }
        List<TagPath> paths = new ArrayList<>(states.size());
        List<TagChangeListener> listeners = new ArrayList<>(states.size());
        for (TagState s : states) {
            paths.add(s.path);
            listeners.add(s.listener);
            s.subscribed = true;
        }
        context.getTagManager().subscribeAsync(paths, listeners).whenComplete((ok, err) -> {
            if (err != null) {
                logger.error("Failed to subscribe to {} tag(s)", paths.size(), err);
                states.forEach(s -> s.subscribed = false);
            } else {
                logger.info("Subscribed to {} tag(s)", paths.size());
            }
        });
    }

    private void unsubscribe(List<TagState> states) {
        if (states.isEmpty()) {
            return;
        }
        List<TagPath> paths = new ArrayList<>(states.size());
        List<TagChangeListener> listeners = new ArrayList<>(states.size());
        for (TagState s : states) {
            if (!s.subscribed) continue;
            paths.add(s.path);
            listeners.add(s.listener);
            s.subscribed = false;
        }
        if (!paths.isEmpty()) {
            context.getTagManager().unsubscribeAsync(paths, listeners).whenComplete((ok, err) -> {
                if (err != null) logger.warn("Error unsubscribing {} tag(s)", paths.size(), err);
            });
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // Readings
    // ---------------------------------------------------------------------------------------------------------

    private void onTagChange(TagState state, TagChangeEvent event) {
        QualifiedValue qv = event.getValue();
        if (qv == null) {
            return;
        }
        state.last = qv;
        if (!running.get() || !state.enabled || state.metricId == null) {
            return;
        }
        long timeMs = qv.getTimestamp() != null ? qv.getTimestamp().getTime() : System.currentTimeMillis();
        emitReading(state, qv.getQuality() != null ? qv.getQuality().getCode() : 0, qv.getValue(), timeMs);
    }

    void emitDueMaxAgeReadings() {
        long now = System.currentTimeMillis();
        for (TagState s : tags.values()) {
            QualifiedValue last = s.last;
            if (!s.enabled || s.maxAgeMs <= 0 || !isGood(last) || s.metricId == null) {
                continue;
            }
            if (now - s.lastEmitMs >= Math.max(MIN_MAX_AGE_MS, s.maxAgeMs)) {
                // A new edge-side reading of the unchanged Good value, timestamped when it was collected.
                emitReading(s, MAX_AGE_QUALITY, last.getValue(), now);
                maxAgeReadings.incrementAndGet();
            }
        }
    }

    private void emitReading(TagState s, int quality, Object value, long timeMs) {
        MetricReading r = readings.create(s.metricId, timeMs * 1000L, quality, value);
        s.lastEmitMs = System.currentTimeMillis();
        if (r == null) {
            if (readingsUnsupported.incrementAndGet() <= 20) {
                logger.warn("Unsupported value type {} on {}; reading not recorded",
                        value == null ? "null" : value.getClass().getName(), s.path);
            }
            return;
        }
        if (buffer.offer(SpoolRecord.newBuilder().setReading(r).build())) {
            readingsCollected.incrementAndGet();
        } else if (readingsRejected.incrementAndGet() % 10_000 == 1) {
            logger.warn("Historian buffer full; {} reading(s) rejected so far", readingsRejected.get());
        }
    }

    /** Readings posted through the HTTP ingest endpoints (for example from Event Streams). */
    public boolean ingest(TagEventPayload payload) {
        if (!running.get() || payload == null || payload.getTagPath() == null) {
            httpRejected.incrementAndGet();
            return false;
        }
        String path = payload.getTagPath();
        if (!path.startsWith("[") && payload.getTagProvider() != null && !payload.getTagProvider().isBlank()) {
            path = "[" + payload.getTagProvider() + "]" + path;
        }
        MetricMetadataEvent binding = tracker.getActive(TagPathParts.parse(path));
        if (binding == null || !collects(binding) || payload.getQualityCode() == null || payload.getTimestamp() == null) {
            httpRejected.incrementAndGet();
            return false;
        }
        Object value = ReadingFactory.coerceToDataType(payload.getValue(), binding.hasDataType() ? binding.getDataType() : null);
        MetricReading r = readings.create(binding.getMetricId(), payload.getTimestamp() * 1000L, payload.getQualityCode(), value);
        if (r == null) {
            readingsUnsupported.incrementAndGet();
            return false;
        }
        if (!buffer.offer(SpoolRecord.newBuilder().setReading(r).build())) {
            readingsRejected.incrementAndGet();
            return false;
        }
        readingsCollected.incrementAndGet();
        return true;
    }

    public int ingestBatch(TagEventPayload[] payloads) {
        int accepted = 0;
        if (payloads != null) {
            for (TagEventPayload p : payloads) {
                if (ingest(p)) accepted++;
            }
        }
        return accepted;
    }

    // ---------------------------------------------------------------------------------------------------------
    // Delivery
    // ---------------------------------------------------------------------------------------------------------

    /** Metadata and collector events are retried until the buffer accepts them; they are never dropped. */
    private void enqueue(SpoolRecord record) {
        pending.add(record);
        drainPending();
    }

    private void drainPending() {
        SpoolRecord r;
        while ((r = pending.peek()) != null) {
            if (!buffer.offer(r)) {
                return;
            }
            pending.poll();
        }
    }

    void flush() {
        try {
            drainPending();
            long deadline = System.currentTimeMillis() + FLUSH_TIME_BUDGET_MS;
            while (System.currentTimeMillis() < deadline) {
                if (!client.ensureOpen()) {
                    return;
                }
                HistorianBuffer.Drain drain = buffer.drain(config.getBatchSize());
                if (drain.isEmpty()) {
                    return;
                }
                List<MetricReading> r = new ArrayList<>();
                List<MetricMetadataEvent> m = new ArrayList<>();
                List<CollectorEvent> c = new ArrayList<>();
                for (SpoolRecord rec : drain.records) {
                    switch (rec.getRecordCase()) {
                        case READING: r.add(rec.getReading()); break;
                        case METADATA: m.add(rec.getMetadata()); break;
                        case COLLECTOR: c.add(rec.getCollector()); break;
                        default: break;
                    }
                }
                if (!client.send(r, m, c)) {
                    return;
                }
                buffer.commit(drain);
                batchesSent.incrementAndGet();
            }
        } catch (Throwable t) {
            logger.error("Historian flush failed", t);
        }
    }

    private SpoolRecord collectorEvent(String type) {
        return SpoolRecord.newBuilder().setCollector(CollectorEvent.newBuilder()
                .setProducerId(producerId)
                .setSessionId(sessionId)
                .setTime(System.currentTimeMillis() * 1000L)
                .setEventType(type)).build();
    }

    // ---------------------------------------------------------------------------------------------------------
    // Diagnostics and helpers
    // ---------------------------------------------------------------------------------------------------------

    public String getDiagnostics() {
        long subscribed = tags.values().stream().filter(s -> s.subscribed).count();
        StringBuilder sb = new StringBuilder();
        sb.append("=== Historian Collector ===\n");
        sb.append("Running: ").append(running.get()).append('\n');
        sb.append("Producer ID: ").append(producerId).append('\n');
        sb.append("Session ID: ").append(sessionId).append('\n');
        if (sourceKeyedRule.getError() != null) {
            sb.append("Source-keyed rule error: ").append(sourceKeyedRule.getError()).append('\n');
        }
        sb.append("Active tags: ").append(tracker.activeCount()).append('\n');
        sb.append("Subscribed (").append(config.isLogAllSelectedTags() ? "all selected tags" : "history enabled").append("): ")
                .append(subscribed).append('\n');
        sb.append("Last metadata scan: ").append(lastScanMs > 0 ? ((System.currentTimeMillis() - lastScanMs) / 1000) + " seconds ago, " : "")
                .append(lastScanSummary).append('\n');
        sb.append("Metadata snapshots emitted: ").append(metadataEmitted.get()).append('\n');
        sb.append("Readings collected: ").append(readingsCollected.get())
                .append(" (max-interval: ").append(maxAgeReadings.get()).append(")\n");
        sb.append("Readings rejected (buffer full): ").append(readingsRejected.get()).append('\n');
        sb.append("Readings with unsupported values: ").append(readingsUnsupported.get()).append('\n');
        sb.append("HTTP ingest rejected: ").append(httpRejected.get()).append('\n');
        sb.append("Pending metadata/collector records: ").append(pending.size()).append('\n');
        sb.append("Buffer: ").append(buffer.isDiskBacked() ? "disk, backlog " + buffer.backlog() + " bytes" : "memory, " + buffer.backlog() + " record(s)")
                .append(", unreadable dropped ").append(buffer.getCorrupt())
                .append(", write failures ").append(buffer.getWriteFailures()).append('\n');
        sb.append("Batches sent: ").append(batchesSent.get()).append('\n');
        sb.append(client.getDiagnostics());
        return sb.toString();
    }

    private boolean collects(MetricMetadataEvent snapshot) {
        return config.isLogAllSelectedTags() || snapshot.getHistoryEnabled();
    }

    /** The tag property read as the source reference: Ignition's opcItemPath unless another property is configured. */
    static Property<?> opcItemPathProperty(ConfigModel config) {
        String name = config.getOpcItemPathProperty();
        if (name == null || name.isBlank() || name.trim().equals(OpcTagTypeProperties.OPCItemPath.getName())) {
            return OpcTagTypeProperties.OPCItemPath;
        }
        return new BasicProperty<>(name.trim(), Object.class);
    }

    /** The folder-mode root, when it is excluded from metric names. */
    static TagPathParts nameRoot(ConfigModel config) {
        String folder = config.getTagFolderPath();
        if (config.isIncludeRootFolderInNames() || !"folder".equals(config.getTagSelectionMode())
                || folder == null || folder.isBlank()) {
            return null;
        }
        TagPathParts root = TagPathParts.parse(folder.trim());
        String path = root.getPath();
        while (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        return TagPathParts.parse(root.getProvider() == null ? path : "[" + root.getProvider() + "]" + path);
    }

    private static String resolveProducerId(GatewayContext context, ConfigModel config) {
        String configured = config.getSourceSystemId();
        if (configured != null && !configured.isBlank()) {
            return configured.trim();
        }
        return context.getSystemPropertiesManager().getSystemName();
    }

    private static boolean isGood(QualifiedValue qv) {
        return qv != null && qv.getQuality() != null && qv.getQuality().isGood();
    }

    private static Object good(QualifiedValue qv) {
        return isGood(qv) ? qv.getValue() : null;
    }

    private static String asString(Object o) {
        if (o == null) return null;
        if (o instanceof Enum) return ((Enum<?>) o).name();
        return o.toString();
    }

    static String toJson(Object value) {
        if (value instanceof DocumentElement) {
            return ((DocumentElement) value).toStringNoIndent();
        }
        if (value instanceof Dataset) {
            Dataset ds = (Dataset) value;
            StringBuilder sb = new StringBuilder("{\"columns\":").append(ReadingFactory.scalarArrayJson(ds.getColumnNames())).append(",\"rows\":[");
            Object[] row = new Object[ds.getColumnCount()];
            for (int r = 0; r < ds.getRowCount(); r++) {
                for (int c = 0; c < row.length; c++) {
                    row[c] = ds.getValueAt(r, c);
                }
                String json = ReadingFactory.scalarArrayJson(row);
                if (json == null) {
                    return null;
                }
                sb.append(r > 0 ? "," : "").append(json);
            }
            return sb.append("]}").toString();
        }
        return ReadingFactory.scalarArrayJson(value);
    }

    private static ThreadFactory daemon(String name) {
        return r -> {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        };
    }

    private final class TagState {
        final TagPath path;
        final TagChangeListener listener;
        volatile String metricId;
        volatile String dataType;
        volatile boolean enabled;
        volatile long maxAgeMs;
        volatile boolean subscribed;
        volatile QualifiedValue last;
        volatile long lastEmitMs;

        TagState(TagPath path) {
            this.path = path;
            this.listener = event -> onTagChange(this, event);
        }
    }
}
