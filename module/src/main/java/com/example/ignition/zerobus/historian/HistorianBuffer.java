package com.example.ignition.zerobus.historian;

import com.example.ignition.zerobus.proto.SpoolRecord;
import com.example.ignition.zerobus.saf.DiskSpool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * FIFO buffer of historian records, backed by memory or by a {@link DiskSpool}.
 *
 * Disk mode stages offered records in memory and writes them to the spool in groups ({@link #persistStaged()}),
 * so a crash can lose at most the staged records. Records leave the buffer only when a drained batch is
 * committed after a successful send. A full buffer rejects new records; rejections are counted, never hidden.
 */
public final class HistorianBuffer {
    private static final Logger logger = LoggerFactory.getLogger(HistorianBuffer.class);

    private final DiskSpool spool;
    private final long maxSpoolBytes;
    private final int memoryCapacity;
    private final long readMaxBytes;

    private final Object lock = new Object();
    private final Object writeLock = new Object();
    private Deque<byte[]> staged = new ArrayDeque<>();
    private long stagedBytes = 0L;
    private final Deque<SpoolRecord> memory;

    private final AtomicLong rejected = new AtomicLong();
    private final AtomicLong corrupt = new AtomicLong();
    private final AtomicLong writeFailures = new AtomicLong();

    private HistorianBuffer(DiskSpool spool, long maxSpoolBytes, int memoryCapacity, long readMaxBytes) {
        this.spool = spool;
        this.maxSpoolBytes = maxSpoolBytes;
        this.memoryCapacity = Math.max(1, memoryCapacity);
        this.readMaxBytes = Math.max(1024L, readMaxBytes);
        this.memory = spool == null ? new ArrayDeque<>() : null;
    }

    public static HistorianBuffer inMemory(int capacity) {
        return new HistorianBuffer(null, 0L, capacity, 0L);
    }

    /**
     * @param stagingCapacity maximum records held in memory between group writes
     */
    public static HistorianBuffer onDisk(Path dir, long maxSpoolBytes, int stagingCapacity, long readMaxBytes) throws IOException {
        return new HistorianBuffer(new DiskSpool(dir), maxSpoolBytes, stagingCapacity, readMaxBytes);
    }

    public boolean isDiskBacked() {
        return spool != null;
    }

    /** @return false when the buffer is full and the record was rejected. */
    public boolean offer(SpoolRecord record) {
        synchronized (lock) {
            if (spool == null) {
                if (memory.size() >= memoryCapacity) {
                    rejected.incrementAndGet();
                    return false;
                }
                memory.addLast(record);
                return true;
            }
            byte[] bytes = record.toByteArray();
            if (staged.size() >= memoryCapacity || spoolBacklog() + stagedBytes + bytes.length + 4 > maxSpoolBytes) {
                rejected.incrementAndGet();
                return false;
            }
            staged.addLast(bytes);
            stagedBytes += bytes.length + 4L;
            return true;
        }
    }

    /** Writes staged records to the spool with a single sync. No-op in memory mode. */
    public void persistStaged() {
        if (spool == null) {
            return;
        }
        synchronized (writeLock) {
            Deque<byte[]> batch;
            synchronized (lock) {
                if (staged.isEmpty()) {
                    return;
                }
                batch = staged;
                staged = new ArrayDeque<>();
                stagedBytes = 0L;
            }
            try {
                spool.appendBatch(new ArrayList<>(batch));
            } catch (IOException e) {
                writeFailures.incrementAndGet();
                logger.warn("Failed writing {} staged record(s) to the spool; keeping them staged for retry", batch.size(), e);
                synchronized (lock) {
                    for (byte[] b : staged) {
                        batch.addLast(b);
                    }
                    staged = batch;
                    stagedBytes = 0L;
                    for (byte[] b : staged) {
                        stagedBytes += b.length + 4L;
                    }
                }
            }
        }
    }

    public Drain drain(int maxRecords) {
        maxRecords = Math.max(1, maxRecords);
        if (spool == null) {
            synchronized (lock) {
                List<SpoolRecord> out = new ArrayList<>(Math.min(maxRecords, memory.size()));
                for (SpoolRecord r : memory) {
                    if (out.size() >= maxRecords) break;
                    out.add(r);
                }
                return new Drain(out, out.size(), out.size());
            }
        }
        persistStaged();
        try {
            DiskSpool.ReadBatch rb = spool.readBatch(maxRecords, readMaxBytes);
            List<SpoolRecord> out = new ArrayList<>(rb.records().size());
            for (byte[] b : rb.records()) {
                try {
                    out.add(SpoolRecord.parseFrom(b));
                } catch (Exception e) {
                    corrupt.incrementAndGet();
                    logger.warn("Dropping unreadable spool record: {}", e.toString());
                }
            }
            return new Drain(out, rb.nextOffset(), rb.records().size());
        } catch (IOException e) {
            logger.warn("Failed reading from the spool", e);
            return new Drain(List.of(), -1L, 0);
        }
    }

    public void commit(Drain drain) {
        if (drain == null || drain.position < 0) {
            return;
        }
        if (spool == null) {
            synchronized (lock) {
                for (long n = drain.position; n > 0 && !memory.isEmpty(); n--) {
                    memory.removeFirst();
                }
            }
            return;
        }
        try {
            spool.commit(drain.position);
        } catch (IOException e) {
            logger.warn("Failed committing the spool read offset; records may be resent", e);
        }
    }

    /** Pending records: spool backlog bytes (disk) or record count (memory), plus staged records. */
    public long backlog() {
        synchronized (lock) {
            return spool == null ? memory.size() : spoolBacklog() + stagedBytes;
        }
    }

    public long getRejected() {
        return rejected.get();
    }

    public long getCorrupt() {
        return corrupt.get();
    }

    public long getWriteFailures() {
        return writeFailures.get();
    }

    private long spoolBacklog() {
        try {
            return spool.backlogBytes();
        } catch (IOException e) {
            return 0L;
        }
    }

    /** A drained batch; commit it only after every record was sent successfully. */
    public static final class Drain {
        public final List<SpoolRecord> records;
        final long position;
        private final int rawCount;

        Drain(List<SpoolRecord> records, long position, int rawCount) {
            this.records = records;
            this.position = position;
            this.rawCount = rawCount;
        }

        /** @return true when nothing was read; a batch of only unreadable records is not empty and must be committed. */
        public boolean isEmpty() {
            return rawCount == 0;
        }
    }
}
