package io.github.tridipmandal1.livedict.backend;

import java.util.List;

/**
 * Abstraction for LiveDict persistence backends.
 *
 * <p>A backend is responsible for storing and retrieving key-value pairs,
 * optionally with TTL information. The in-memory cache layer (LiveDict)
 * sits on top of the backend and handles:
 * <ul>
 *   <li>Fast reads (cache hits don't touch the backend)</li>
 *   <li>Lazy expiry checks</li>
 *   <li>Event dispatch</li>
 * </ul>
 * <p>The backend handles:
 * <ul>
 *   <li>Durable storage (memory, disk, remote server)</li>
 *   <li>Initialization (loading existing data on startup)</li>
 *   <li>Optional eager expiry cleanup (database-side TTL enforcement)</li>
 * </ul>
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public interface Backend <K, V> {

    /**
     * Stores a key-value pair with optional expiry.
     *
     * <p>If an entry with the same key already exists, it is replaced.
     *
     * @param key        the key — must not be null
     * @param value      the value to store
     * @param expiresAt  the absolute expiry timestamp in milliseconds since epoch,
     *                   or {@code -1} for no expiry
     * @throws BackendException if the operation fails (I/O error, serialization failure, etc.)
     */
    void put(K key, V value, long expiresAt) throws BackendException;

    /**
     * Retrieves the value for the given key.
     *
     * <p>If the key does not exist, or if it exists but has expired
     * (checked against current time), returns {@code null}.
     *
     * @param key the key to look up
     * @return the value, or {@code null} if not found or expired
     * @throws BackendException if the operation fails
     */
    V get(K key) throws BackendException;

    /**
     * Deletes the entry for the given key.
     *
     * <p>If the key does not exist, this is a no-op (does not throw).
     *
     * @param key the key to delete
     * @throws BackendException if the operation fails
     */
    void delete(K key) throws BackendException;

    /**
     * Loads all non-expired entries from the backend.
     *
     * <p>This is called during LiveDict initialization to warm the in-memory
     * cache from persistent storage. Expired entries
     * should be filtered out and not included in the result.
     *
     * @return a map of all non-expired key-value pairs
     * @throws BackendException if the operation fails
     */
    List<LoadEntry<K, V>> loadAll() throws BackendException;

    /**
     * Removes all entries from the backend.
     *
     * <p>This is called when {@code LiveDict.clear()} is invoked.
     *
     * @throws BackendException if the operation fails
     */
    void clear() throws BackendException;

    /**
     * Releases any resources held by this backend (connections, file handles, etc.).
     *
     * <p>Called when {@code LiveDict.close()} is invoked.
     *
     * @throws BackendException if cleanup fails
     */
    void close() throws BackendException;

    /**
     * Persists multiple key-value pairs in a single operation.
     *
     * <p>This is called by the write-behind executor to batch write for efficiency.
     *
     * <p><b>Default implementation:</b> Falls back to sequential {@link #put} calls.
     *
     * @param entries the entries to persist
     * @throws BackendException if any write fails
     */
    default void putBatch(List<PutEntry<K, V>> entries) throws BackendException{
        for (PutEntry<K, V> entry : entries) {
            put(entry.key, entry.value, entry.expiresAt);
        }
    }

    /**
     * Deletes multiple keys in a single operation.
     *
     * <p><b>Default implementation:</b> Falls back to sequential {@link #delete} calls.
     *
     * @param keys the keys to delete
     * @throws BackendException if any delete fails
     */
    default void deleteBatch(List<K> keys) throws BackendException{
        for (K key : keys) {
            delete(key);
        }
    }


    /**
     * A loaded entry with TTL metadata from the backend.
     *
     * @param key       the key
     * @param value     the value
     * @param expiresAt absolute expiry timestamp in millis, or -1 for no expiry
     */
    record LoadEntry<K, V>(K key, V value, long expiresAt){}
    /**
     * A key-value pair with expiry metadata for batch operations.
     *
     * @param key       the key
     * @param value     the value
     * @param expiresAt absolute expiry timestamp in millis, or -1 for no expiry
     */
    record PutEntry<K, V>(K key, V value, long expiresAt) {}
}
