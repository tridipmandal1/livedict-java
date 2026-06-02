package io.github.tridipmandal1.livedict.core;

import java.time.Instant;

/**
 * Internal wrapper that associates a stored value with its expiry metadata.
 *
 * <p>This class is <em>package-private</em> — users of the library never
 * interact with it directly. It is an implementation detail of LiveDict.
 *
 * @param <V> the type of the stored value
 */
public final class LiveDictEntry<V> {

    private final V value;

    /**
     * The absolute moment this entry expires.
     * {@code null} means this entry has no expiry (lives forever).
     */

    private final Instant expiresAt;

    /**
     * Creates a new entry with a TTL.
     *
     * @param value     the value to store — may be null if the user explicitly stores null
     * @param ttlMillis time-to-live in milliseconds; use -1 for no expiry
     */
    LiveDictEntry(V value, long ttlMillis) {
        this.value = value;
        this.expiresAt = (ttlMillis > 0) ?
                Instant.now().plusMillis(ttlMillis) : null;
    }

    /**
     * @return the stored value
     */
    public V getValue() {
        return value;
    }

    /**
     * Checks whether this entry has passed its TTL deadline.
     *
     * <p>If the entry has no expiry ({@code expiresAt == null}), this always
     * returns {@code false} — the entry is never expired.
     *
     * @return {@code true} if the entry's TTL has elapsed and it should be removed
     */
    public boolean isExpired() {
        return expiresAt != null && Instant.now().isAfter(expiresAt);
    }

    /**
     * @return the expiry deadline, or {@code null} if this entry has no TTL
     */

    public Instant getExpiresAt() {
        return expiresAt;
    }

    /**
     * @return {@code true} if this entry will never expire
     */
    boolean isImmoral() {
        return expiresAt == null;
    }
}
