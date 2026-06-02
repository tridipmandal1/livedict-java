package io.github.tridipmandal1.livedict.writebehind;

import io.github.tridipmandal1.livedict.backend.Backend;
import io.github.tridipmandal1.livedict.backend.BackendException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


/**
 * Background executor that drains a queue of write operations and batches them
 * for efficient backend persistence.
 *
 * <p>In write-behind mode, {@code LiveDict} enqueues operations here instead of
 * blocking on backend writes. This thread periodically drains the queue and
 * executes operations in batches, providing much higher throughput.
 *
 * <p><b>Flush triggers:</b>
 * <ul>
 *   <li>Batch size reached (e.g., 100 operations queued)</li>
 *   <li>Time interval elapsed (e.g., 100 ms since last flush)</li>
 *   <li>Explicit flush on {@code close()}</li>
 * </ul>
 *
 * <p><b>Thread safety:</b> The writer thread is single-threaded. Multiple threads
 * can enqueue operations concurrently via the thread-safe {@link BlockingQueue}.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public class WriteBehindExecutor<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(WriteBehindExecutor.class);

    private final Backend<K, V> backend;
    private final BlockingQueue<WriteOperation<K, V>> queue;
    private final Thread writerThread;
    private final int batchSize;
    private final long flushIntervalMs;
    private volatile boolean running;

    /**
     * Creates a write-behind executor.
     *
     * @param backend           the backend to persist to
     * @param queueSize         max queue capacity (operations rejected when full)
     * @param batchSize         max operations per batch
     * @param flushIntervalMs   max time between flushes
     */
    public WriteBehindExecutor(Backend<K, V> backend,
                               int queueSize,
                               int batchSize,
                               long flushIntervalMs) {
        this.backend = backend;
        this.queue = new ArrayBlockingQueue<>(queueSize);
        this.batchSize = batchSize;
        this.flushIntervalMs = flushIntervalMs;
        this.running = true;

        this.writerThread = new Thread(this::writerLoop, "livedict-write-behind");
        this.writerThread.setDaemon(true);
    }

    /**
     * Starts the background writer thread.
     */
    public void start() {
        writerThread.start();
        LOGGER.info("WriteBehindExecutor started (batch={}, interval={}ms)", batchSize, flushIntervalMs);
    }

    /**
     * Enqueues a write operation.
     *
     * <p>Returns immediately if space is available. If the queue is full,
     * the operation is rejected and {@code false} is returned.
     *
     * @param operation the operation to enqueue
     * @return {@code true} if enqueued, {@code false} if queue was full
     */
    public boolean enqueue(WriteOperation<K, V> operation){
        boolean accepted = queue.offer(operation);
        if (!accepted) {
            LOGGER.warn("Write-behind queue full — operation rejected: {}", operation);
        }
        return accepted;
    }

    /**
     * Stops the writer thread and flushes any pending operations.
     *
     * <p>Blocks until all queued operations are processed or timeout elapses.
     *
     * @param timeoutMillis max time to wait for flush
     */
    public void stop(long timeoutMillis) {
        running = false;
        writerThread.interrupt();
        try {
            writerThread.join(timeoutMillis);
            if (writerThread.isAlive()) {
                LOGGER.warn("Writer thread did not stop cleanly within timeout");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // Ensure any remaining operations are flushed
        flush();
    }

    /**
     * The writer thread's main loop.
     *
     * <p>Periodically drains the queue and executes batches. Runs until
     * {@code running} is set to false.
     *
     * <p><b>Flush triggers:</b>
     * <ul>
     *   <li>Batch size reached (e.g., 100 items in queue)</li>
     *   <li>Flush interval elapsed (e.g., 100ms since last flush)</li>
     * </ul>
     */
    private void writerLoop() {
        long lastFlushTime = System.currentTimeMillis();

        while (running || !queue.isEmpty()) {
            try {
                // How long until we must flush due to time?
                long now = System.currentTimeMillis();
                long timeSinceLastFlush = now - lastFlushTime;
                long timeUntilFlush = flushIntervalMs - timeSinceLastFlush;

                // Check if we should flush now
                boolean batchFull = queue.size() >= batchSize;
                boolean intervalElapsed = timeUntilFlush <= 0;
                boolean hasItems = !queue.isEmpty();

                if (hasItems && (batchFull || intervalElapsed)) {
                    // Flush trigger met - drain and execute
                    List<WriteOperation<K, V>> batch = new ArrayList<>(batchSize);
                    queue.drainTo(batch, batchSize);

                    if (!batch.isEmpty()) {
                        executeBatch(batch);
                        lastFlushTime = System.currentTimeMillis();
                        LOGGER.info("Flushed batch of {} operations (batchFull={}, intervalElapsed={})", batch.size(), batchFull, intervalElapsed);
                    }
                } else {
                    // Sleep briefly and check again
                    // Use shorter sleep to be responsive to new items
                    long sleepMs = Math.min(Math.max(timeUntilFlush, 10), 50);
                    Thread.sleep(sleepMs);
                }

            } catch (InterruptedException e) {
                if (!running) {
                    break;  // shutdown signal
                }
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                LOGGER.info("Unexpected error in write-behind loop", e);
            }
        }
        LOGGER.info("WriteBehindExecutor stopped");
    }

    /**
     * Immediately flushes all queued operations.
     */
    private void flush() {
        List<WriteOperation<K, V>> remaining = new ArrayList<>(queue.size());
        queue.drainTo(remaining);

        if (!remaining.isEmpty()) {
            executeBatch(remaining);
            LOGGER.info("Flushed {} remaining operations", remaining.size());
        }
    }

    /**
     * Executes a batch of operations.
     *
     * <p>Groups operations by type (Put vs Delete vs Clear) and calls
     * the appropriate batch method on the backend.
     */
    private void executeBatch(List<WriteOperation<K, V>> operations) {
        if (operations.isEmpty()) {
            return;
        }

        List<Backend.PutEntry<K, V>> putOps = new ArrayList<>();
        List<K> deleteOps = new ArrayList<>();
        boolean hasClear = false;

        for (WriteOperation<K, V> op: operations) {

            switch (op) {
                case WriteOperation.Put<K, V> put ->
                        putOps.add(new Backend.PutEntry<>(put.key(), put.value(), put.expiresAt()));
                case WriteOperation.Delete<K, V> delete ->
                        deleteOps.add(delete.key());
                case WriteOperation.Clear<K, V> clear ->
                        hasClear = true;
            }
        }

        try {

            // Execute clear first if present (invalidates everything)
            if (hasClear) {
                backend.clear();
            }

            // then deletes and puts
            if (!deleteOps.isEmpty()) {
                backend.deleteBatch(deleteOps);
            }

            if (!putOps.isEmpty()) {
                backend.putBatch(putOps);
            }

            LOGGER.info("Executed batch: {} puts, {} deletes, clear={}", putOps.size(), deleteOps.size(), hasClear);
        } catch (BackendException e) {
            LOGGER.error("Backend batch operation failed — data may be inconsistent", e);
            // Continue running — don't let one batch failure kill the executor
        }


    }
}
