package com.livedict.core;

import java.time.Instant;

public final class LiveDictEntry<V> {

    private final V value;

    private final Instant expiresAt;

    LiveDictEntry(V value, long ttlMillis) {
        this.value = value;
        this.expiresAt = (ttlMillis > 0) ?
                Instant.now().plusMillis(ttlMillis) : null;
    }

    public V getValue() {
        return value;
    }

    public boolean isExpired() {
        return expiresAt != null && Instant.now().isAfter(expiresAt);
    }

    public Instant getExpiresAt() {
        return expiresAt;
    }

    boolean isImmoral() {
        return expiresAt == null;
    }
}
