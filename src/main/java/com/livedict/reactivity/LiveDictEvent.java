package com.livedict.reactivity;

import java.time.Instant;

public final class LiveDictEvent<K, V> {

    private final K key;
    private final V value;
    private final EventType type;
    private final Instant occurredAt;

    public LiveDictEvent(EventType type, K key, V value) {
        this.type = type;
        this.key = key;
        this.value = value;
        this.occurredAt = Instant.now();
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    public EventType getType() {
        return type;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    @Override
    public String toString() {
        return "LiveDictEvent{" +
                "type=" + type +
                ", key=" + key +
                ", value=" + value +
                ", occurredAt=" + occurredAt +
                '}';
    }
}
