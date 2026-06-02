package io.github.tridipmandal1.livedict.reactivity;

import java.time.Instant;

/**
 * Immutable snapshot of a LiveDict lifecycle event.
 *
 * <p>This is the object passed to every registered {@link LiveDictListener}.
 * It captures the key, the value at the time of the event, which event
 * occurred, and when it occurred.
 *
 * <p><b>Why immutable?</b> Listeners may run on a different thread than the
 * caller. If the event object were mutable, a listener could accidentally
 * (or maliciously) modify it while another listener is reading it.
 * Making all fields final eliminates that class of bug entirely.
 *
 * @param <K> the type of key in the LiveDict instance
 * @param <V> the type of value in the LiveDict instance
 */
public final class LiveDictEvent<K, V> {

    private final K key;
    private final V value; // May be null on ON_DELETE if value was already gone
    private final EventType type;
    private final Instant occurredAt;

    /**
     * Constructs a new event. Called internally by LiveDict — users never
     * construct these directly.
     *
     * @param type       the lifecycle event that occurred
     * @param key        the key involved
     * @param value      the value at the time of the event (nullable)
     */
    public LiveDictEvent(EventType type, K key, V value) {
        this.type = type;
        this.key = key;
        this.value = value;
        this.occurredAt = Instant.now();
    }

    /** @return the key involved in this event */
    public K getKey() {
        return key;
    }

    /**
     * @return the value at the time of the event. May be {@code null} if the
     *         entry had already been removed before the event was dispatched.
     */
    public V getValue() {
        return value;
    }

    /** @return the type of event that occurred */
    public EventType getType() {
        return type;
    }

    /** @return the exact moment this event was created */
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
