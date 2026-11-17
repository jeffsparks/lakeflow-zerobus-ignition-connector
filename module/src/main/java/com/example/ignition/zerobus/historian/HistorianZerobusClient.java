package com.example.ignition.zerobus.historian;

import com.databricks.zerobus.StreamConfigurationOptions;
import com.databricks.zerobus.TableProperties;
import com.databricks.zerobus.ZerobusSdk;
import com.databricks.zerobus.ZerobusStream;
import com.example.ignition.zerobus.ConfigModel;
import com.example.ignition.zerobus.proto.CollectorEvent;
import com.example.ignition.zerobus.proto.MetricMetadataEvent;
import com.example.ignition.zerobus.proto.MetricReading;
import com.example.ignition.zerobus.telemetry.DatabricksPartnerTelemetry;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Zerobus client for the three historian landing tables, sharing one connection configuration.
 */
public final class HistorianZerobusClient {
    private static final Logger logger = LoggerFactory.getLogger(HistorianZerobusClient.class);

    public static final String READINGS_TABLE = "metrics_ingest";
    public static final String METADATA_TABLE = "metric_metadata_events";
    public static final String COLLECTOR_TABLE = "collector_events";

    private static final long MAX_BACKOFF_MS = 60_000L;
    private static final long MAX_AUTH_BACKOFF_MS = 10L * 60_000L;

    private final ConfigModel config;
    private final TableStream<MetricReading> readings;
    private final TableStream<MetricMetadataEvent> metadata;
    private final TableStream<CollectorEvent> collector;

    private ZerobusSdk sdk;
    private int consecutiveFailures = 0;
    private long nextAttemptAtMs = 0L;
    private volatile String lastError;
    private volatile long lastSuccessMs = 0L;

    public HistorianZerobusClient(ConfigModel config) {
        this.config = config;
        String schema = config.getTargetSchema() == null ? "" : config.getTargetSchema().trim();
        this.readings = new TableStream<>(schema + "." + READINGS_TABLE, MetricReading.getDefaultInstance());
        this.metadata = new TableStream<>(schema + "." + METADATA_TABLE, MetricMetadataEvent.getDefaultInstance());
        this.collector = new TableStream<>(schema + "." + COLLECTOR_TABLE, CollectorEvent.getDefaultInstance());
    }

    /**
     * Sends one drained batch. Records of each type are sent in order on their table's stream.
     *
     * @return true only when every record was acknowledged as durable
     */
    public synchronized boolean send(List<MetricReading> readingBatch, List<MetricMetadataEvent> metadataBatch,
                                     List<CollectorEvent> collectorBatch) {
        if (!ensureOpen()) {
            return false;
        }
        try {
            metadata.send(metadataBatch);
            readings.send(readingBatch);
            collector.send(collectorBatch);
            consecutiveFailures = 0;
            lastSuccessMs = System.currentTimeMillis();
            return true;
        } catch (Exception e) {
            recordFailure(e);
            logger.warn("Zerobus send failed; the batch will be retried: {}", lastError);
            return false;
        }
    }

    /** @return true when all streams are open, attempting (re)connection subject to backoff. */
    public synchronized boolean ensureOpen() {
        if (!config.isEnabled()) {
            return false;
        }
        if (sdk != null && readings.isOpened() && metadata.isOpened() && collector.isOpened()) {
            return true;
        }
        long now = System.currentTimeMillis();
        if (now < nextAttemptAtMs) {
            return false;
        }
        try {
            if (sdk == null) {
                DatabricksPartnerTelemetry.ensureRegistered();
                sdk = new ZerobusSdk(config.getZerobusEndpoint(), config.getWorkspaceUrl());
            }
            StreamConfigurationOptions options = streamOptions();
            for (TableStream<?> s : List.of(readings, metadata, collector)) {
                s.open(sdk, options);
            }
            boolean open = readings.isOpened() && metadata.isOpened() && collector.isOpened();
            if (open) {
                consecutiveFailures = 0;
                logger.info("Zerobus streams open for {}, {}, {}", readings.table, metadata.table, collector.table);
            } else {
                recordFailure(new IllegalStateException("stream not OPENED"));
            }
            return open;
        } catch (Exception e) {
            recordFailure(e);
            logger.warn("Could not open Zerobus streams: {}", lastError);
            return false;
        }
    }

    /** Opens and closes a stream to every table; verifies the endpoint, credentials, tables and permissions. */
    public boolean testConnection() {
        try {
            DatabricksPartnerTelemetry.ensureRegistered();
            ZerobusSdk testSdk = new ZerobusSdk(config.getZerobusEndpoint(), config.getWorkspaceUrl());
            StreamConfigurationOptions options = StreamConfigurationOptions.builder()
                    .setMaxInflightRecords(100).setRecovery(false).setFlushTimeoutMs(10_000).build();
            for (TableStream<?> s : List.of(readings, metadata, collector)) {
                s.probe(testSdk, options);
            }
            return true;
        } catch (Exception e) {
            lastError = describe(e);
            logger.error("Zerobus connection test failed: {}", lastError);
            return false;
        }
    }

    public void shutdown() {
        List<TableStream<?>> streams;
        synchronized (this) {
            streams = List.of(readings, metadata, collector);
            sdk = null;
        }
        Thread t = new Thread(() -> streams.forEach(TableStream::close), "Zerobus-Historian-Close");
        t.setDaemon(true);
        t.start();
        try {
            t.join(5_000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public String getDiagnostics() {
        StringBuilder sb = new StringBuilder();
        for (TableStream<?> s : List.of(readings, metadata, collector)) {
            sb.append("Stream ").append(s.table).append(": ").append(s.state()).append(", sent=").append(s.sent).append('\n');
        }
        sb.append("Last successful send: ")
                .append(lastSuccessMs > 0 ? ((System.currentTimeMillis() - lastSuccessMs) / 1000) + " seconds ago" : "never")
                .append('\n');
        sb.append("Consecutive failures: ").append(consecutiveFailures).append('\n');
        if (lastError != null) {
            sb.append("Last error: ").append(lastError).append('\n');
        }
        return sb.toString();
    }

    private StreamConfigurationOptions streamOptions() {
        return StreamConfigurationOptions.builder()
                .setMaxInflightRecords(Math.max(config.getBatchSize() * 2, 1000))
                .setRecovery(true)
                .setRecoveryTimeoutMs((int) config.getConnectionTimeoutMs())
                .setRecoveryBackoffMs((int) config.getRetryBackoffMs())
                .setRecoveryRetries(config.getMaxRetries())
                .setFlushTimeoutMs((int) config.getRequestTimeoutMs())
                .setServerLackOfAckTimeoutMs((int) config.getRequestTimeoutMs())
                .build();
    }

    private void recordFailure(Throwable t) {
        consecutiveFailures++;
        lastError = describe(t);
        boolean auth = isAuthFailure(t);
        long base = Math.max(250L, config.getRetryBackoffMs());
        long backoff = auth
                ? Math.min(Math.max(30_000L, base) << Math.min(5, consecutiveFailures - 1), MAX_AUTH_BACKOFF_MS)
                : Math.min(base << Math.min(6, consecutiveFailures - 1), MAX_BACKOFF_MS);
        nextAttemptAtMs = System.currentTimeMillis() + backoff + ThreadLocalRandom.current().nextLong(250L);
        for (TableStream<?> s : List.of(readings, metadata, collector)) {
            s.markFailed();
        }
    }

    private static boolean isAuthFailure(Throwable t) {
        StringBuilder msg = new StringBuilder();
        for (Throwable c = t; c != null && msg.length() < 4096; c = c.getCause()) {
            if (c.getMessage() != null) msg.append(c.getMessage().toLowerCase(Locale.ROOT)).append('\n');
        }
        String m = msg.toString();
        return m.contains("unauthorized") || m.contains("forbidden") || m.contains("401") || m.contains("403")
                || m.contains("invalid_client") || m.contains("invalid_grant") || m.contains("client_secret");
    }

    private static String describe(Throwable t) {
        Throwable root = t;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();
        return root.getClass().getSimpleName() + (root.getMessage() == null ? "" : ": " + root.getMessage());
    }

    private final class TableStream<T extends Message> {
        final String table;
        final TableProperties<T> properties;
        ZerobusStream<T> stream;
        boolean failed = false;
        long sent = 0L;

        TableStream(String table, T defaultInstance) {
            this.table = table;
            this.properties = new TableProperties<>(table, defaultInstance);
        }

        boolean isOpened() {
            return stream != null && !failed && "OPENED".equals(String.valueOf(stream.getState()));
        }

        String state() {
            return stream == null ? "not created" : String.valueOf(stream.getState()) + (failed ? " (failed)" : "");
        }

        void open(ZerobusSdk zerobus, StreamConfigurationOptions options) throws Exception {
            if (isOpened()) {
                return;
            }
            long timeout = config.getConnectionTimeoutMs();
            CompletableFuture<ZerobusStream<T>> future = (stream == null)
                    ? zerobus.createStream(properties, config.getOauthClientId(), config.getOauthClientSecret(), options)
                    : zerobus.recreateStream(stream);
            stream = future.get(timeout, TimeUnit.MILLISECONDS);
            failed = false;
        }

        void probe(ZerobusSdk zerobus, StreamConfigurationOptions options) throws Exception {
            ZerobusStream<T> s = zerobus.createStream(properties, config.getOauthClientId(), config.getOauthClientSecret(), options)
                    .get(10, TimeUnit.SECONDS);
            s.close();
        }

        void send(List<T> records) throws Exception {
            if (records == null || records.isEmpty()) {
                return;
            }
            if (!isOpened()) {
                throw new IllegalStateException(table + " stream is " + state());
            }
            List<CompletableFuture<Void>> futures = new ArrayList<>(records.size());
            for (T r : records) {
                futures.add(stream.ingestRecord(r));
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(config.getRequestTimeoutMs(), TimeUnit.MILLISECONDS);
            stream.flush();
            sent += records.size();
        }

        void markFailed() {
            failed = true;
        }

        void close() {
            ZerobusStream<T> s = stream;
            if (s == null) {
                return;
            }
            try {
                s.close();
            } catch (Exception e) {
                logger.debug("Error closing {} stream", table, e);
            }
        }
    }
}
