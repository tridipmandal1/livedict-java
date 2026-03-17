package com.livedict.core;

import com.livedict.backend.Backend;
import com.livedict.backend.BackendException;
import com.livedict.backend.PersistenceMode;
import com.livedict.expiry.ExpiryScheduler;
import com.livedict.reactivity.EventType;
import com.livedict.reactivity.LiveDictEvent;
import com.livedict.reactivity.LiveDictListener;
import com.livedict.writebehind.WriteBehindExecutor;
import com.livedict.writebehind.WriteOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;


public class LiveDict<K, V> implements AutoCloseable{

    private static final Logger LOGGER =
            LoggerFactory.getLogger(LiveDict.class);

    private final ConcurrentHashMap<K, LiveDictEntry<V>> store;

    private final LiveDictConfig config;

    private final Map<EventType, CopyOnWriteArrayList<LiveDictListener<K,V>>> listeners;

    private final ExpiryScheduler<K,V> expiryScheduler;

    private final ExecutorService listenerExecutor;

    private final ConcurrentHashMap<K, ReentrantLock> keyLocks;

    private final Backend<K, V> backend;

    private final WriteBehindExecutor<K, V> writeBehindExecutor;

    public LiveDict (){
        this(LiveDictConfig.defaults());
    }

    public LiveDict(LiveDictConfig config) {
        this.config = config;
        this.store = new ConcurrentHashMap<>();
        this.listeners = new ConcurrentHashMap<>();
        this.keyLocks = new ConcurrentHashMap<>();
        this.backend = config.getBackend();

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

        // sync the memory store with the backend

        try {
            Map<K, V> existing = backend.loadAll();
            if (!existing.isEmpty()) {
                // TODO: Backend not returning ttl information
                for (Map.Entry<K, V> entry : existing.entrySet()) {
                    store.put(entry.getKey(), new LiveDictEntry<>(entry.getValue(), -1));
                }
                LOGGER.info("Loaded {} entries from backend", existing.size());
            }
        } catch (BackendException e) {
            LOGGER.error("Failed to load existing entries from backend - starting with empty cache", e);
        }

        if (config.getPersistenceMode() == PersistenceMode.WRITE_BEHIND) {
            this.writeBehindExecutor =
                    new WriteBehindExecutor<>(
                            backend,
                            config.getWriteBehindQueueSize(),
                            config.getWriteBehindBatchSize(),
                            config.getWriteBehindFlushIntervalMillis()
                    );
            this.writeBehindExecutor.start();
            LOGGER.info("Write-behind mode enabled");
        } else {
            this.writeBehindExecutor = null;
            LOGGER.info("Write-through mode enabled");
        }

        this.expiryScheduler = new ExpiryScheduler<>(store, this::handleExpiredEntry);
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

        long expiresAt = (ttlMillis > 0)
                ? System.currentTimeMillis() + ttlMillis
                : -1;

        if (writeBehindExecutor != null) {
            store.put(key, entry);
            boolean enqueued = writeBehindExecutor.enqueue(
                    new WriteOperation.Put<>(key, value, expiresAt)
            );
            if (!enqueued) {
                // memory updated but backend write rejected
                // Reject&Log strategy
                LOGGER.warn("Write-behind queue full — write may be lost for key: {}", key);
            }
        } else {
            store.put(key, entry);
            try {
                backend.put(key, value, expiresAt);
            } catch (BackendException e) {
                store.remove(key, entry); // rollback
                LOGGER.error("Backend write failed for key: {}", key, e);

                throw new RuntimeException("Backend persistence failed", e);
            }

        }
        fireEvent(EventType.ON_SET, key, value);
    }

    public Optional<V> get(K key) {
        if (key == null) return Optional.empty();

        LiveDictEntry<V> entry = store.get(key);

        if (entry == null) return Optional.empty();

        if (entry.isExpired()) {
            removeEntry(key, entry, EventType.ON_EXPIRE);
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

        LiveDictEntry<V> removed = store.get(key);
        if (removed != null) {

            if (writeBehindExecutor != null) {
                store.remove(key);
                writeBehindExecutor.enqueue(
                        new WriteOperation.Delete<>(key)
                );
            } else {
                try {
                    backend.delete(key);
                    store.remove(key);
                } catch (BackendException e) {
                    LOGGER.trace("Backend delete failed for key: {}", key, e);
                    return false;
                }
            }
            fireEvent(EventType.ON_DELETE, key, removed.getValue());
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

        if (writeBehindExecutor != null) {
            writeBehindExecutor.enqueue(
                    new WriteOperation.Clear<>()
            );
        } else {
            try {
                backend.clear();
            } catch (BackendException e) {
                LOGGER.warn("Backend clear failed", e);
            }
        }
    }

    public Set<K> keys() {
        return Collections.unmodifiableSet(store.keySet());
    }

    // Event listener APIs

    public void on(EventType type, LiveDictListener<K, V> listener) {
        if (type == null) throw new NullPointerException("EventType must not be null");
        if (listener == null) throw new NullPointerException("Listener must not be null");
        listeners.get(type).add(listener);
    }

    public boolean removeListener(EventType type, LiveDictListener<K, V> listener) {
        if (type == null || listener == null) return false;
        return listeners.get(type).remove(listener);
    }

    // internal event dispatch
    private void fireEvent(EventType type, K key, V value) {

        List<LiveDictListener<K, V>> listenersForType = listeners.get(type);
        if(listenersForType.isEmpty()) return;

        LiveDictEvent<K, V> event = new LiveDictEvent<>(type, key, value);

        if (listenerExecutor != null) {
            //async listener
            listenerExecutor.submit(() -> invokeListeners(listenersForType, event, type, key));
        }
         else {
             //sync listener
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
                LOGGER.warn("LiveDict listener threw Throwable for event {} on key {}",
                        type, key, t);
            }
        }
    }

    private void fireExpireEvent(K key, V value){
        fireEvent(EventType.ON_EXPIRE, key, value);
    }

    private void handleExpiredEntry(K key, V value) {

        if (writeBehindExecutor != null) {
            writeBehindExecutor.enqueue(
                    new WriteOperation.Delete<>(key)
            );
        }else {
            try {
                backend.delete(key);
            } catch (BackendException e) {
                LOGGER.warn("Backend delete failed during scheduled expiry for key: {}", key, e);
            }
        }
        fireEvent(EventType.ON_EXPIRE, key, value);
    }

    private void removeEntry(K key, LiveDictEntry<V> entry, EventType eventType) {

        boolean removed = store.remove(key, entry);

        if (removed) {
            if (writeBehindExecutor != null) {
                writeBehindExecutor.enqueue(
                        new WriteOperation.Delete<>(key)
                );
            } else {
                try {
                    backend.delete(key);
                } catch (BackendException e) {
                    LOGGER.warn("Backend delete failed during expiry for key: {}", key, e);
                }
            }
        }
        fireEvent(eventType, key, entry.getValue());
    }

    // per-key locking apis

    public void lock(K key) throws InterruptedException{
        if (key == null) throw new NullPointerException("Lock key must not be null");
        getLockForKey(key).lockInterruptibly();
    }

    public boolean lock(K key, long timeout, TimeUnit unit) throws InterruptedException{
        if (key == null) throw new NullPointerException("Key must not be null");
        return getLockForKey(key).tryLock(timeout, unit);
    }

    public boolean tryLock(K key) {
        if (key == null) throw new NullPointerException("Key must not be null");
        return getLockForKey(key).tryLock();
    }

    public void unlock(K key) {
        if (key == null) throw new NullPointerException("Key must not be null");
        ReentrantLock lock = keyLocks.get(key);
        if (lock == null) throw new IllegalMonitorStateException(
                "Attempt to unlock key: " + key + " which has never been locked"
        );

        lock.unlock();
    }

    public boolean isLocked(K key) {
        if (key == null) throw new NullPointerException("Key must not be null");
        ReentrantLock lock = keyLocks.get(key);
        return lock != null && lock.isLocked();
    }

    private ReentrantLock getLockForKey(K key) {
        return keyLocks.computeIfAbsent(key, k -> new ReentrantLock());
    }

    // lifecycle

    @Override
    public void close() {
        expiryScheduler.stop();

        if (listenerExecutor != null) {
            listenerExecutor.shutdown();

            try {
                if (!listenerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    LOGGER.trace("Listener executor didn't terminate within 5s - forcing shutdown");
                    listenerExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                listenerExecutor.shutdownNow();
            }
        }

        if (writeBehindExecutor != null) {
            writeBehindExecutor.stop(5_000);
        }
        try {
            backend.close();
        } catch (BackendException e) {
            LOGGER.error("Failed to close backend", e);
        }
    }

    @Override
    public String toString() {
        return
                "LiveDict{size=" + store.size() + "}";
    }
}
