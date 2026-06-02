package io.github.tridipmandal1.livedict.reactivity;

/**
 * Represents the lifecycle moments in a LiveDict entry's life.
 *
 * <p>Users register listeners against these event types to be notified
 * when something meaningful happens to a key-value pair.
 *
 * <p>Design note: Using an enum (not string constants) means the compiler
 * catches typos at compile time — {@code ON_SETT} is a compile error,
 * not a silent no-op at runtime.
 */
public enum EventType {

    /**
     * Fired when a key-value pair is inserted or updated via {@code set()}.
     */
    ON_SET,

    /**
     * Fired when a key is successfully retrieved via {@code get()}.
     * Not fired on a miss (key not found or already expired).
     */
    ON_GET,

    /**
     * Fired when a key's TTL elapses and it is removed from the store.
     * This can be triggered by the background reaper OR by a lazy expiry
     * check inside {@code get()}.
     */
    ON_EXPIRE,

    /**
     * Fired when a key is explicitly removed via {@code delete()}.
     */
    ON_DELETE,

    /**
     * Fired when a key is automatically removed due to size-based eviction.
     * This happens when the cache reaches its {@code maxSize} and needs to
     * make room for a new entry by evicting the least-recently used (LRU) entry.
     */
    ON_EVICT
}
