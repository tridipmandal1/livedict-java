package com.livedict.backend;

import java.util.List;
import java.util.Map;

public interface Backend <K, V> {

    void put(K key, V value, long expiresAt) throws BackendException;

    V get(K key) throws BackendException;

    void delete(K key) throws BackendException;

    Map<K, V> loadAll() throws BackendException;

    void clear() throws BackendException;

    void close() throws BackendException;

    default void putBatch(List<PutEntry<K, V>> entries) throws BackendException{
        for (PutEntry<K, V> entry : entries) {
            put(entry.key, entry.value, entry.expiresAt);
        }
    }

    default void deleteBatch(List<K> keys) throws BackendException{
        for (K key : keys) {
            delete(key);
        }
    }


    record PutEntry<K, V>(K key, V value, long expiresAt) {}
}
