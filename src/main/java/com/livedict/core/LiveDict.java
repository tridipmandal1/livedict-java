package com.livedict.core;

import com.livedict.expiry.ExpiryScheduler;
import com.livedict.reactivity.EventType;
import com.livedict.reactivity.LiveDictEvent;
import com.livedict.reactivity.LiveDictListener;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LiveDict<K, V> implements AutoCloseable{

    private static final Logger LOGGER =
            Logger.getLogger(LiveDict.class.getName());

    private final ConcurrentHashMap<K, LiveDictEntry<V>> store;

    private final LiveDictConfig config;

    private final Map<EventType, CopyOnWriteArrayList<LiveDictListener<K,V>>> listeners;

    private final ExpiryScheduler<K,V> expiryScheduler;

    private final ExecutorService listenerExecutor;

    public LiveDict (){
        this(LiveDictConfig.defaults());
    }

    public LiveDict(LiveDictConfig config) {
        this.config = config;
        this.store = new ConcurrentHashMap<>();
        this.listeners = new ConcurrentHashMap<>();

        for(EventType type: EventType.values()) {
            listeners.put(type, new CopyOnWriteArrayList<>());
        }

        if (config.isAsyncListeners()) {
            this.listenerExecutor = Executors.newFixedThreadPool(
                    config.getListenerThreads(),
                    runnable -> {
                        Thread thread = new Thread(runnable, "livedict-listener");
                        thread.setDaemon(true);
                        return thread;
                    }
            );

        } else {
            this.listenerExecutor = null;
        }

        this.expiryScheduler = new ExpiryScheduler<>(store, this::fireExpireEvent);
        this.expiryScheduler.start(config.getCleanupIntervalMillis());
    }

    public void set(K key, V value) {
        set(key, value, config.getDefaultTtlMillis());
    }

    public void set(K key, V value, long ttl, TimeUnit unit) {
        if (ttl <= 0) throw new IllegalArgumentException(
                "TTL must be positive. Got: " + ttl
        );
        set(key, value, unit.toMillis(ttl));
    }

    private void set(K key, V value, long ttlMillis) {
        if (key == null) throw new NullPointerException("LiveDict key must not be null");
        LiveDictEntry<V> entry = new LiveDictEntry<>(value, ttlMillis);
        store.put(key, entry);
        fireEvent(EventType.ON_SET, key, value);
    }

    public Optional<V> get(K key) {
        if (key == null) return Optional.empty();

        LiveDictEntry<V> entry = store.get(key);

        if (entry == null) return Optional.empty();

        if (entry.isExpired()) {
            store.remove(key);
            fireExpireEvent(key, entry.getValue());
            return Optional.empty();
        }

        fireEvent(EventType.ON_GET, key, entry.getValue());
        return Optional.ofNullable(entry.getValue());
    }

    public V getOrDefault(K key, V defaultValue){
        return get(key).orElse(defaultValue);
    }

    public boolean delete(K key) {
        if (key == null) return false;

        LiveDictEntry<V> toRemove = store.remove(key);
        if (toRemove != null) {
            fireEvent(EventType.ON_DELETE, key, toRemove.getValue());
            return true;
        }
        return false;
    }

    public boolean exists(K key) {
        return get(key).isPresent();
    }

    public int size() {
        return store.size();
    }

    public void clear() {
        store.clear();
    }

    public Set<K> keys() {
        return Collections.unmodifiableSet(store.keySet());
    }


    public void on(EventType type, LiveDictListener<K, V> listener) {
        if (type == null) throw new NullPointerException("EventType must not be null");
        if (listener == null) throw new NullPointerException("Listener must not be null");
        listeners.get(type).add(listener);
    }

    public boolean removeListener(EventType type, LiveDictListener<K, V> listener) {
        if (type == null || listener == null) return false;
        return listeners.get(type).remove(listener);
    }

    private void fireEvent(EventType type, K key, V value) {

        List<LiveDictListener<K, V>> listenersForType = listeners.get(type);
        if(listenersForType.isEmpty()) return;

        LiveDictEvent<K, V> event = new LiveDictEvent<>(type, key, value);

        if (listenerExecutor != null) {
            listenerExecutor.submit(() -> invokeListeners(listenersForType, event, type, key));
        }
         else {
             invokeListeners(listenersForType, event, type, key);
        }

    }

    private void invokeListeners(List<LiveDictListener<K, V>> listenersForType,
                                 LiveDictEvent<K, V> event,
                                 EventType type,
                                 K key) {
        for (LiveDictListener<K, V> listener: listenersForType) {
            try {
                listener.onEvent(event);
            } catch (Throwable t) {
                LOGGER.log(Level.WARNING,
                        "LiveDict listener threw Throwable for event " +
                                type + " on key " + key, t
                );
            }
        }
    }

    private void fireExpireEvent(K key, V value){
        fireEvent(EventType.ON_EXPIRE, key, value);
    }


    @Override
    public void close() {
        expiryScheduler.stop();

        if (listenerExecutor != null) {
            listenerExecutor.shutdown();

            try {
                if (!listenerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    LOGGER.warning("Listener executor didn't terminate within 5s - forcing shutdown");
                    listenerExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                listenerExecutor.shutdownNow();
            }
        }
    }

    @Override
    public String toString() {
        return
                "LiveDict{size=" + store.size() + "}";
    }
}
