package com.livedict.writebehind;

public sealed interface WriteOperation<K, V> permits WriteOperation.Put,
        WriteOperation.Delete, WriteOperation.Clear {

    record Put<K, V>(K key, V value, long expiresAt) implements WriteOperation<K, V> {}

    record Delete<K, V>(K key) implements WriteOperation<K, V> {}

    record Clear<K, V>() implements WriteOperation<K, V> {}
}
