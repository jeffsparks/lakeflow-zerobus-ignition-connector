package com.example.ignition.zerobus.historian;

import com.databricks.zerobus.ZerobusProtoStream;
import com.databricks.zerobus.ZerobusSdk;
import com.example.ignition.zerobus.ConfigModel;
import com.example.ignition.zerobus.proto.CollectorEvent;
import com.example.ignition.zerobus.proto.MetricMetadataEvent;
import com.example.ignition.zerobus.proto.MetricReading;
import com.example.ignition.zerobus.telemetry.DatabricksPartnerTelemetry;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Locale;
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
    // Token requests intermittently fail with 401 invalid_authorization_details and succeed on the next try, so
    // only repeated auth failures get the long auth backoff.
    private static final int AUTH_FAILURES_BEFORE_LONG_BACKOFF = 3;

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
        this.readings = new TableStream<>(schema + "." + READINGS_TABLE, MetricReading.getDescriptor().toProto());
        this.metadata = new TableStream<>(schema + "." + METADATA_TABLE, MetricMetadataEvent.getDescriptor().toProto());
        this.collector = new TableStream<>(schema + "." + COLLECTOR_TABLE, CollectorEvent.getDescriptor().toProto());
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
                sdk = newSdk();
            }
            for (TableStream<?> s : List.of(readings, metadata, collector)) {
                s.open(sdk);
            }
            boolean open = readings.isOpened() && metadata.isOpened() && collector.isOpened();
            if (open) {
                consecutiveFailures = 0;
                logger.info("Zerobus streams open for {}, {}, {} (applicationName={})", readings.table, metadata.table,
                        collector.table, DatabricksPartnerTelemetry.ingestApplicationName());
            } else {
                recordFailure(new IllegalStateException("stream not open"));
            }
            return open;
        } catch (Exception e) {
            recordFailure(e);
            logger.warn("Could not open Zerobus streams (retry in {} ms): {}",
                    Math.max(0L, nextAttemptAtMs - System.currentTimeMillis()), lastError);
            return false;
        }
    }

    /**
     * Opens and closes a stream to every table; verifies the endpoint, credentials, tables and permissions.
     * On failure {@link #getLastError()} names the table and the cause.
     */
    public boolean testConnection() {
        DatabricksPartnerTelemetry.ensureRegistered();
        ZerobusSdk testSdk = null;
        try {
            testSdk = newSdk();
            for (TableStream<?> s : List.of(readings, metadata, collector)) {
                s.probe(testSdk);
            }
            return true;
        } catch (Exception e) {
            lastError = e instanceof TableException ? e.getMessage() : describe(e);
            logger.error("Zerobus connection test failed: {}", lastError);
            return false;
        } finally {
            closeQuietly(testSdk);
        }
    }

    public String getLastError() {
        return lastError;
    }

    public void shutdown() {
        List<TableStream<?>> streams;
        ZerobusSdk sdkToClose;
        synchronized (this) {
            streams = List.of(readings, metadata, collector);
            sdkToClose = sdk;
            sdk = null;
        }
        Thread t = new Thread(() -> {
            streams.forEach(TableStream::close);
            closeQuietly(sdkToClose);
        }, "Zerobus-Historian-Close");
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

    private ZerobusSdk newSdk() {
        return new ZerobusSdk(config.getZerobusEndpoint(), config.getWorkspaceUrl(),
                DatabricksPartnerTelemetry.ingestApplicationName());
    }

    private static void closeQuietly(ZerobusSdk s) {
        if (s == null) {
            return;
        }
        try {
            s.close();
        } catch (Exception e) {
            logger.debug("Error closing Zerobus SDK", e);
        }
    }

    private static int ms(long value, int fallback) {
        return value <= 0 ? fallback : (int) Math.min(Integer.MAX_VALUE, value);
    }

    private void recordFailure(Throwable t) {
        consecutiveFailures++;
        lastError = t instanceof TableException ? t.getMessage() : describe(t);
        boolean auth = isAuthFailure(t) && consecutiveFailures >= AUTH_FAILURES_BEFORE_LONG_BACKOFF;
        long base = Math.max(250L, config.getRetryBackoffMs());
        long backoff = auth
                ? Math.min(Math.max(30_000L, base) << Math.min(5, consecutiveFailures - AUTH_FAILURES_BEFORE_LONG_BACKOFF),
                        MAX_AUTH_BACKOFF_MS)
                : Math.min(base << Math.min(6, consecutiveFailures - 1), MAX_BACKOFF_MS);
        nextAttemptAtMs = System.currentTimeMillis() + backoff + ThreadLocalRandom.current().nextLong(250L);
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

    /** A stream failure, with the table name prefixed to the root cause. */
    private static final class TableException extends Exception {
        TableException(String table, Throwable cause) {
            super(table + ": " + describe(cause), cause);
        }
    }

    private final class TableStream<T extends Message> {
        final String table;
        final DescriptorProto descriptor;
        ZerobusProtoStream stream;
        boolean failed = false;
        long sent = 0L;

        TableStream(String table, DescriptorProto descriptor) {
            this.table = table;
            this.descriptor = descriptor;
        }

        boolean isOpened() {
            return stream != null && !failed && !stream.isClosed();
        }

        String state() {
            if (stream == null) {
                return "not created";
            }
            return (stream.isClosed() ? "CLOSED" : "OPENED") + (failed ? " (failed)" : "");
        }

        /** Opens a fresh stream; a failed batch is resent whole, so unacknowledged records are not replayed. */
        void open(ZerobusSdk zerobus) throws Exception {
            if (isOpened()) {
                return;
            }
            close();
            stream = null;
            try {
                stream = zerobus.streamBuilder()
                        .table(table)
                        .oauth(config.getOauthClientId(), config.getOauthClientSecret())
                        .maxInflightRecords(Math.max(config.getBatchSize() * 2, 1000))
                        .recovery(true)
                        .recoveryTimeoutMs(ms(config.getConnectionTimeoutMs(), 15_000))
                        .recoveryBackoffMs(ms(config.getRetryBackoffMs(), 1_000))
                        .recoveryRetries(Math.max(0, config.getMaxRetries()))
                        .flushTimeoutMs(ms(config.getRequestTimeoutMs(), 60_000))
                        .serverLackOfAckTimeoutMs(ms(config.getRequestTimeoutMs(), 60_000))
                        .compiledProto(descriptor)
                        .build()
                        .get(config.getConnectionTimeoutMs(), TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                failed = true;
                throw new TableException(table, e);
            }
            failed = false;
        }

        void probe(ZerobusSdk zerobus) throws Exception {
            ZerobusProtoStream s;
            try {
                s = zerobus.streamBuilder()
                        .table(table)
                        .oauth(config.getOauthClientId(), config.getOauthClientSecret())
                        .maxInflightRecords(100)
                        .recovery(false)
                        .flushTimeoutMs(10_000)
                        .compiledProto(descriptor)
                        .build()
                        .get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new TableException(table, e);
            }
            s.close();
        }

        void send(List<T> records) throws Exception {
            if (records == null || records.isEmpty()) {
                return;
            }
            if (!isOpened()) {
                throw new IllegalStateException(table + " stream is " + state());
            }
            try {
                stream.ingestRecordsOffset(records);
                stream.flush();
            } catch (Exception e) {
                failed = true;
                throw new TableException(table, e);
            }
            sent += records.size();
        }

        void close() {
            ZerobusProtoStream s = stream;
            if (s == null || s.isClosed()) {
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
