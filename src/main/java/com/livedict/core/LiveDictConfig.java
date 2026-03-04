package com.livedict.core;


import com.livedict.backend.Backend;
import com.livedict.backend.MemoryBackend;

import java.util.concurrent.TimeUnit;

@SuppressWarnings("All")
public final class LiveDictConfig {

    private final long defaultTtlMillis;

    private final long cleanupIntervalMillis;

    private final boolean asyncListeners;

    private final int listenerThreads;

    private final Backend<?, ?> backend;

    private LiveDictConfig(Builder builder) {
        this.defaultTtlMillis = builder.defaultTtlMillis;
        this.cleanupIntervalMillis = builder.cleanupIntervalMillis;
        this.asyncListeners = builder.asyncListeners;
        this.listenerThreads = builder.listenerThreads;
        this.backend = builder.backend;
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

        public LiveDictConfig build() {
            if (backend == null) {
                backend = new MemoryBackend<>();
            }
            return new LiveDictConfig(this);
        }
    }
}
