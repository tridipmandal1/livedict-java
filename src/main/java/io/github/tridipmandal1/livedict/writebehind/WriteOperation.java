package io.github.tridipmandal1.livedict.writebehind;


/**
 * Sealed interface representing a pending backend write operation.
 *
 * <p>In write-behind mode, instead of blocking on every {@code set()} or {@code delete()},
 * LiveDict enqueues a {@code WriteOperation} and returns immediately. A background
 * writer thread drains the queue and executes operations in batches.
 *
 * <p><b>Sealed interface pattern:</b> Java 17+ feature that restricts which classes
 * can implement this interface. Only the three types below are permitted:
 * <ul>
 *   <li>{@link Put} — persist a key-value pair</li>
 *   <li>{@link Delete} — remove a key</li>
 *   <li>{@link Clear} — remove all keys</li>
 * </ul>
 *
 * This makes pattern matching exhaustive (compiler knows all possible types).
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public sealed interface WriteOperation<K, V> permits WriteOperation.Put,
        WriteOperation.Delete, WriteOperation.Clear {

    /**
     * Persist a key-value pair to the backend.
     *
     * @param key       the key to persist
     * @param value     the value to persist
     * @param expiresAt absolute expiry timestamp in millis, or -1 for no expiry
     */
    record Put<K, V>(K key, V value, long expiresAt) implements WriteOperation<K, V> {}

    /**
     * Delete a key from the backend.
     *
     * @param key the key to delete
     */
    record Delete<K, V>(K key) implements WriteOperation<K, V> {}

    /**
     * Clear all keys from the backend.
     */
    record Clear<K, V>() implements WriteOperation<K, V> {}
}
