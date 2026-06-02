package io.github.tridipmandal1.livedict.core;


import io.github.tridipmandal1.livedict.backend.Backend;
import io.github.tridipmandal1.livedict.backend.MemoryBackend;
import io.github.tridipmandal1.livedict.backend.PersistenceMode;
import io.github.tridipmandal1.livedict.backend.SQLiteBackend;

import java.util.concurrent.TimeUnit;


/**
 * Immutable configuration for a {@link LiveDict} instance.
 *
 * <p>Use {@link Builder} to construct an instance:
 *
 * <pre>{@code
 *   LiveDictConfig config = LiveDictConfig.builder()
 *       .defaultTtl(30, TimeUnit.SECONDS)
 *       .cleanupInterval(10, TimeUnit.SECONDS)
 *       .backend(new SqliteBackend<>("/tmp/cache.db"))
 *       .build();
 *
 *   LiveDict<String, User> cache = new LiveDict<>(config);
 * }</pre>
 */
@SuppressWarnings("All")
public final class LiveDictConfig {

    /**
     * Default TTL applied to entries that don't specify one.
     * -1 means "no default TTL" — entries live forever unless explicitly given a TTL.
     */
    private final long defaultTtlMillis;

    /**
     * How often the background reaper thread scans for and removes expired entries.
     */
    private final long cleanupIntervalMillis;

    /**
     * Whether listeners are invoked asynchronously on a dedicated thread pool.
     * When false (default), listeners run synchronously on the calling thread.
     */
    private final boolean asyncListeners;

    /**
     * Number of threads in the listener executor pool (only used when asyncListeners=true).
     * Default is 2 — listeners are typically lightweight and don't need many threads.
     */
    private final int listenerThreads;

    /**
     * The persistence backend for storing entries.
     * Default is {@link MemoryBackend} (no persistence).
     */
    private final Backend<?, ?> backend;

    /**
     * Persistence mode — how writes are persisted to the backend.
     * Default is WRITE_THROUGH.
     */
    private final PersistenceMode persistenceMode;

    /**
     * Maximum number of write operations to batch before flushing to backend.
     * Only used when persistenceMode = WRITE_BEHIND.
     */
    private final int writeBehindBatchSize;

    /**
     * Maximum time to wait before flushing queued writes to backend, in milliseconds.
     * Only used when persistenceMode = WRITE_BEHIND.
     */
    private final long writeBehindFlushIntervalMillis;

    /**
     * Maximum size of the write-behind queue.
     * When full, new writes are rejected with a logged warning.
     * Only used when persistenceMode = WRITE_BEHIND.
     */
    private final int writeBehindQueueSize;

    private LiveDictConfig(Builder builder) {
        this.defaultTtlMillis = builder.defaultTtlMillis;
        this.cleanupIntervalMillis = builder.cleanupIntervalMillis;
        this.asyncListeners = builder.asyncListeners;
        this.listenerThreads = builder.listenerThreads;
        this.backend = builder.backend;
        this.persistenceMode = builder.persistenceMode;
        this.writeBehindBatchSize = builder.writeBehindBatchSize;
        this.writeBehindFlushIntervalMillis = builder.writeBehindFlushIntervalMillis;
        this.writeBehindQueueSize = builder.writeBehindQueueSize;
    }

    public long getDefaultTtlMillis() {
        return defaultTtlMillis;
    }

    public long getCleanupIntervalMillis() {
        return cleanupIntervalMillis;
    }

    public boolean isAsyncListeners() {
        return asyncListeners;
    }

    public int getListenerThreads() {
        return listenerThreads;
    }

    public PersistenceMode getPersistenceMode() {
        return persistenceMode;
    }

    public int getWriteBehindBatchSize() {
        return writeBehindBatchSize;
    }

    public long getWriteBehindFlushIntervalMillis() {
        return writeBehindFlushIntervalMillis;
    }

    public int getWriteBehindQueueSize() {
        return writeBehindQueueSize;
    }

    @SuppressWarnings("unchecked")
    public <K, V> Backend<K, V> getBackend() {
        return (Backend<K, V>) backend;
    }

    /** @return a new Builder with sensible defaults pre-filled */
    public static Builder builder(){
        return new Builder();
    }

    /** @return a default config — no TTL, cleanup every 60 seconds */
    public static LiveDictConfig defaults() {
        return builder().build();
    }

    public static final class Builder{

        private long defaultTtlMillis = -1; // no default TTL
        private long cleanupIntervalMillis = 60_000; // 60 seconds
        private boolean asyncListeners = false; // default is sync
        private int listenerThreads = 2; // default is 2
        private Backend<?, ?> backend = null; // default is memory backend
        // write-behind settings
        private PersistenceMode persistenceMode = PersistenceMode.WRITE_THROUGH; // default is write-through
        private int writeBehindBatchSize = 100; // batch up to 100 writes
        private long writeBehindFlushIntervalMillis = 100; // // flush every 100ms
        private int writeBehindQueueSize = 10_000; // bounded queue

        private Builder(){}

        /**
         * Sets the default TTL for entries that don't provide their own.
         *
         * @param amount   the numeric amount
         * @param unit     the time unit (e.g., {@code TimeUnit.SECONDS})
         * @return this builder, for chaining
         */
        public Builder defaultTtl(long amount, TimeUnit unit) {
            if (amount <= 0) throw new IllegalArgumentException("Default TTL must be positive");
            this.defaultTtlMillis = unit.toMillis(amount);
            return this;
        }

        /**
         * Sets how frequently the background reaper thread scans for expired keys.
         * More frequent = lower memory waste, but slightly more CPU overhead.
         *
         * @param amount   the numeric amount
         * @param unit     the time unit
         * @return this builder, for chaining
         */
        public Builder cleanupInterval(long amount, TimeUnit unit) {
            if (amount <= 0) throw new IllegalArgumentException("Cleanup time interval must be positive");
            this.cleanupIntervalMillis = unit.toMillis(amount);
            return this;
        }

        /**
         * Enables asynchronous listener dispatch.
         *
         * <p>When enabled, all registered listeners are invoked on a dedicated
         * thread pool rather than on the caller's thread. This prevents slow
         * listeners from blocking cache operations like {@code set()} and {@code get()}.
         *
         * <p><b>Trade-offs:</b>
         * <ul>
         *   <li><b>Pro:</b> {@code set()} returns immediately even with slow listeners</li>
         *   <li><b>Pro:</b> Listeners can do I/O (logging, metrics) without stalling callers</li>
         *   <li><b>Con:</b> Listeners fire slightly later (microseconds to milliseconds)</li>
         *   <li><b>Con:</b> Event ordering across different event types is not guaranteed</li>
         * </ul>
         *
         * <p>Default: {@code false} (synchronous dispatch).
         *
         * @param enabled whether to use async listener dispatch
         * @return this builder, for chaining
         */
        public Builder asyncListeners(boolean enabled) {
            this.asyncListeners = enabled;
            return this;
        }

        /**
         * Sets the number of threads in the async listener executor pool.
         * Only used when {@link #asyncListeners(boolean)} is enabled.
         *
         * <p>Default: 2 threads (sufficient for most workloads).
         *
         * @param threads the thread pool size
         * @return this builder, for chaining
         */
        public Builder listenerThreads(int threads) {
            if (threads < 1) throw new IllegalArgumentException("Listener threads must be >1");
            this.listenerThreads = threads;
            return this;
        }

        /**
         * Sets a custom persistence backend.
         *
         * <p>The backend determines where entries are stored:
         * <ul>
         *   <li>{@link MemoryBackend} — in-memory only (default, no persistence)</li>
         *   <li>{@link SQLiteBackend} — local file persistence via SQLite</li>
         *   <li>RedisBackend — remote persistence via Redis</li>
         * </ul>
         *
         * <p><b>Example:</b>
         * <pre>{@code
         *   LiveDictConfig config = LiveDictConfig.builder()
         *       .backend(new SqliteBackend<>("/tmp/cache.db"))
         *       .build();
         * }</pre>
         *
         * <p>Default: {@link MemoryBackend} (no persistence).
         *
         * @param backend the backend implementation
         * @return this builder, for chaining
         */
        public Builder backend(Backend<?, ?> backend) {
            this.backend = backend;
            return this;
        }

        /**
         * Sets the persistence mode for backend writes.
         *
         * <p><b>WRITE_THROUGH (default):</b> Every {@code set()} blocks until backend
         * confirms. Strong consistency, higher latency (~1-5ms).
         *
         * <p><b>WRITE_BEHIND:</b> Writes enqueued and batched. Operations return
         * immediately (~50μs). Eventual consistency, much higher throughput.
         *
         * <p>Default: {@code WRITE_THROUGH} for backward compatibility with v1.1.
         *
         * @param mode the persistence mode
         * @return this builder, for chaining
         */
        public Builder persistenceMode(PersistenceMode persistenceMode) {
            this.persistenceMode = persistenceMode;
            return this;
        }

        /**
         * Sets the maximum batch size for write-behind mode.
         *
         * <p>When the queue reaches this many operations, they're flushed to the
         * backend immediately (even if the flush interval hasn't elapsed).
         *
         * <p>Only used when {@code persistenceMode = WRITE_BEHIND}.
         *
         * <p>Default: 100 operations.
         *
         * @param batchSize the max batch size
         * @return this builder, for chaining
         */
        public Builder writeBehindBatchSize(int batchSize) {
            if (batchSize < 1) throw new IllegalArgumentException("Write-behind batch size must be more than one");
            this.writeBehindBatchSize = batchSize;
            return this;
        }

        /**
         * Sets the maximum time before flushing queued writes to the backend.
         *
         * <p>Even if the batch size hasn't been reached, queued writes are flushed
         * after this interval elapses.
         *
         * <p>Only used when {@code persistenceMode = WRITE_BEHIND}.
         *
         * <p>Default: 100 milliseconds.
         *
         * @param interval the flush interval amount
         * @param unit     the time unit
         * @return this builder, for chaining
         */
        public Builder writeBehindFlushInterval(long interval, TimeUnit unit) {
            if (interval <= 0) throw new IllegalArgumentException("Write-behind flush interval must be positive");
            this.writeBehindFlushIntervalMillis = unit.toMillis(interval);
            return this;
        }

        /**
         * Sets the maximum size of the write-behind queue.
         *
         * <p>When the queue is full, new write operations are rejected and a
         * warning is logged. This prevents unbounded memory growth.
         *
         * <p>Only used when {@code persistenceMode = WRITE_BEHIND}.
         *
         * <p>Default: 10,000 operations.
         *
         * @param queueSize the queue capacity
         * @return this builder, for chaining
         */
        public Builder writeBehindQueueSize(int queueSize) {
            if (queueSize < 1) throw new IllegalArgumentException("Write-behind queue size must be more than one");
            this.writeBehindQueueSize = queueSize;
            return this;
        }

        /** @return an immutable {@link LiveDictConfig} from this builder's state */
        public LiveDictConfig build() {
            if (backend == null) {
                // default to in-memory backend if not specified
                backend = new MemoryBackend<>();
            }
            return new LiveDictConfig(this);
        }
    }
}
