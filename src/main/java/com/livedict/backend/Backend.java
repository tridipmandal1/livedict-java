package com.livedict.backend;

import java.util.Map;

public interface Backend <K, V> {

    void put(K key, V value, long expiresAt) throws BackendException;

    V get(K key) throws BackendException;

    void delete(K key) throws BackendException;

    Map<K, V> loadAll() throws BackendException;

    void clear() throws BackendException;

    void close() throws BackendException;


}
