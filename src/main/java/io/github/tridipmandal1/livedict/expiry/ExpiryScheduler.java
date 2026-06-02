package io.github.tridipmandal1.livedict.expiry;

import io.github.tridipmandal1.livedict.core.LiveDictEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;
import java.util.function.BiConsumer;


/**
 * Manages periodic expiry scanning for a LiveDict instance.
 *
 * <p>This class runs a background thread on a fixed schedule. Each tick,
 * it iterates over all entries in the store and removes any that have
 * passed their TTL deadline. It then fires {@code ON_EXPIRE} events for
 * each removed key.
 *
 * <p><b>Eager vs Lazy expiry — why both?</b>
 * <ul>
 *   <li><b>Lazy expiry:</b> Inside {@code get()}, we check if the retrieved
 *       entry is expired before returning it. This ensures stale values are
 *       never returned to callers.</li>
 *   <li><b>Eager expiry (this class):</b> The background reaper periodically
 *       evicts expired entries. This reclaims memory even for keys that are
 *       never {@code get()}'d again after their TTL.</li>
 * </ul>
 *
 * <p><b>Thread safety:</b> The {@link ScheduledExecutorService} used here
 * is a single-threaded scheduler. It runs one cleanup task at a time,
 * never concurrently with itself. The store it operates on is a
 * {@link ConcurrentHashMap}, which is safe for concurrent modification.
 *
 * @param <K> the key type of the owning LiveDict
 * @param <V> the value type of the owning LiveDict
 */
public class ExpiryScheduler<K,V> {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(ExpiryScheduler.class);

    /**
     * A single-threaded scheduler. "Single-threaded" is deliberate — cleanup
     * tasks don't need parallelism, and keeping it single-threaded avoids
     * concurrent cleanup runs stomping on each other.
     * The thread is also set as a daemon thread (via the factory below),
     * meaning the JVM can exit even if this thread is still running — it
     * won't keep the application alive just because the cache exists.
     */
    private final ScheduledExecutorService scheduler;

    private ScheduledFuture<?> scheduledTask;

    // These are supplied by the owning LiveDict:
    private final ConcurrentHashMap<K, LiveDictEntry<V>> store;
    private final BiConsumer<K, V> onExpireCallback;  // called with (key, value) when entry is reaped

    /**
     * Constructs the scheduler.
     *
     * @param store            the backing map to scan — entries are removed directly
     * @param onExpireCallback a callback to invoke for each reaped entry, so
     *                         LiveDict can fire the {@code ON_EXPIRE} listener chain
     */
    public ExpiryScheduler(ConcurrentHashMap<K, LiveDictEntry<V>> store,
                           BiConsumer<K, V> onExpireCallback) {
        this.store = store;
        this.onExpireCallback = onExpireCallback;

        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable ->{
            Thread thread = new Thread(runnable, "livedict-expiry-reaper");
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * Starts the background reaper on a fixed-rate schedule.
     *
     * <p>{@code scheduleAtFixedRate} means: run the task every
     * {@code intervalMillis} milliseconds regardless of how long the previous
     * run took. If a run takes longer than the interval, the next run starts
     * immediately after (no overlap).
     *
     * @param intervalMillis how often to scan, in milliseconds
     */
    public void start(long intervalMillis) {
        scheduledTask = scheduler.scheduleAtFixedRate(
            this::reapExpiredEntries,
                intervalMillis, // initial delay
                intervalMillis,
                TimeUnit.MILLISECONDS
        );
    }

    /**
     * Stops the background reaper and releases the scheduler thread.
     *
     * <p>This is called when the user invokes {@code LiveDict.close()}.
     * It's important to shut this down cleanly to avoid thread leaks in
     * environments that create and destroy many LiveDict instances
     * (e.g., tests, or short-lived request-scoped caches).
     */
    public void stop() {
        if (scheduledTask != null) {
            scheduledTask.cancel(false); // false = don't interrupt if mid-run
        }

        scheduler.shutdown();
        LOGGER.info("LiveDict expiry reaper stopped.");
    }

    /**
     * The core reaping logic. Called by the scheduler on each tick.
     *
     * <p>We use {@link ConcurrentHashMap#entrySet()} iteration here.
     * This is safe — {@code ConcurrentHashMap} allows removal during
     * iteration without throwing {@code ConcurrentModificationException}
     * (unlike a regular {@code HashMap}).
     */
    private void reapExpiredEntries() {
        try {
            for(Map.Entry<K, LiveDictEntry<V>> entry: store.entrySet()) {
                LiveDictEntry<V> dictEntry = entry.getValue();

                if (dictEntry.isExpired()) {
                    K key = entry.getKey();
                    boolean removed = store.remove(key, dictEntry);
                    if (removed) {
                        onExpireCallback.accept(key, dictEntry.getValue());
                    }
                }
            }
        } catch (Throwable t) {
            LOGGER.error("Throwable caught during Livedict expiry reaping", t);
        }
    }
}
