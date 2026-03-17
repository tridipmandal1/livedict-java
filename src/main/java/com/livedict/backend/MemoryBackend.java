package com.livedict.backend;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * In-memory backend — no persistence, entries lost on JVM shutdown.
 * <p>This is the default backend for LiveDict </p>
 * It stores entries in a {@link ConcurrentHashMap} with no disk I/O.
 * <p><b>Use cases: </b></p>
 * <ul>
 *      <li>High-performance caching where persistence is not needed</li>
 *      <li>Temporary data (session tokens, rate limit counters)</li>
 * </ul>
 *
 * <p><b>Thread safety: </b>Fully Thread-safe via {@code ConcurrentHashMap}</p>
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class MemoryBackend<K,V> implements Backend<K, V> {

    private static class Entry<V> {
        final V value;
        final long expiresAt; // -1 if never expires

        Entry(V value, long expiresAt) {
            this.value = value;
            this.expiresAt = expiresAt;
        }

        boolean isExpired() {
            return expiresAt != -1 && System.currentTimeMillis() > expiresAt;
        }
    }

    private final ConcurrentHashMap<K, Entry<V>> store = new ConcurrentHashMap<>();
    @Override
    public void put(K key, V value, long expiresAt) throws BackendException {
            store.put(key, new Entry<>(value, expiresAt));
    }

    @Override
    public V get(K key) throws BackendException {
        Entry<V> entry = store.get(key);
        if (entry == null) {
            return null;
        }

        if (entry.isExpired()){ // lazy removal
            store.remove(key);
            return null;
        }

        return entry.value;
    }

    @Override
    public void delete(K key) throws BackendException {
        store.remove(key);
    }

    /**
    * On initialization, load all non-expired entries
     */

    @Override
    public Map<K, V> loadAll() throws BackendException {
        Map<K, V> result = new HashMap<>();
        store.forEach((key, entry) -> {
            if (!entry.isExpired()) {
                result.put(key, entry.value);
            }
        });
        return result;
    }

    @Override
    public void clear() throws BackendException {
        store.clear();
    }

    @Override
    public void close() throws BackendException {
        // no-op
    }
}
