package com.livedict.core;


import java.util.concurrent.TimeUnit;

public final class LiveDictConfig {

    private final long defaultTtlMillis;

    private final long cleanupIntervalMillis;

    private LiveDictConfig(Builder builder) {
        this.defaultTtlMillis = builder.defaultTtlMillis;
        this.cleanupIntervalMillis = builder.cleanupIntervalMillis;
    }

    public long getDefaultTtlMillis() {
        return defaultTtlMillis;
    }

    public long getCleanupIntervalMillis() {
        return cleanupIntervalMillis;
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

        public LiveDictConfig build() {
            return new LiveDictConfig(this);
        }
    }
}
