package com.livedict.core;


import com.livedict.backend.Backend;
import com.livedict.backend.MemoryBackend;
import com.livedict.backend.PersistenceMode;

import java.util.concurrent.TimeUnit;

@SuppressWarnings("All")
public final class LiveDictConfig {

    private final long defaultTtlMillis;

    private final long cleanupIntervalMillis;

    private final boolean asyncListeners;

    private final int listenerThreads;

    private final Backend<?, ?> backend;

    private final PersistenceMode persistenceMode;

    private final int writeBehindBatchSize;

    private final long writeBehindFlushIntervalMillis;

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

    public static Builder builder(){
        return new Builder();
    }

    public static LiveDictConfig defaults() {
        return builder().build();
    }

    public static final class Builder{

        private long defaultTtlMillis = -1;
        private long cleanupIntervalMillis = 60_000;
        private boolean asyncListeners = false;
        private int listenerThreads = 2;
        private Backend<?, ?> backend = null;
        private PersistenceMode persistenceMode = PersistenceMode.WRITE_THROUGH;
        private int writeBehindBatchSize = 100;
        private long writeBehindFlushIntervalMillis = 100;
        private int writeBehindQueueSize = 10_000;

        private Builder(){}

        public Builder defaultTtl(long amount, TimeUnit unit) {
            if (amount <= 0) throw new IllegalArgumentException("Default TTL must be positive");
            this.defaultTtlMillis = unit.toMillis(amount);
            return this;
        }

        public Builder cleanupInterval(long amount, TimeUnit unit) {
            if (amount <= 0) throw new IllegalArgumentException("Cleanup time interval must be positive");
            this.cleanupIntervalMillis = unit.toMillis(amount);
            return this;
        }

        public Builder asyncListeners(boolean enabled) {
            this.asyncListeners = enabled;
            return this;
        }

        public Builder listenerThreads(int threads) {
            if (threads < 1) throw new IllegalArgumentException("Listener threads must be >1");
            this.listenerThreads = threads;
            return this;
        }

        public Builder backend(Backend<?, ?> backend) {
            this.backend = backend;
            return this;
        }

        public Builder persistenceMode(PersistenceMode persistenceMode) {
            this.persistenceMode = persistenceMode;
            return this;
        }

        public Builder writeBehindBatchSize(int batchSize) {
            if (batchSize < 1) throw new IllegalArgumentException("Write-behind batch size must be more than one");
            this.writeBehindBatchSize = batchSize;
            return this;
        }

        public Builder writeBehindFlushInterval(long interval, TimeUnit unit) {
            if (interval <= 0) throw new IllegalArgumentException("Write-behind flush interval must be positive");
            this.writeBehindFlushIntervalMillis = unit.toMillis(interval);
            return this;
        }

        public Builder writeBehindQueueSize(int queueSize) {
            if (queueSize < 1) throw new IllegalArgumentException("Write-behind queue size must be more than one");
            this.writeBehindQueueSize = queueSize;
            return this;
        }

        public LiveDictConfig build() {
            if (backend == null) {
                backend = new MemoryBackend<>();
            }
            return new LiveDictConfig(this);
        }
    }
}
