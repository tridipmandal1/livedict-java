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

    private final ExecutorService asyncExecutor;

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

        //async API executor
        this.asyncExecutor =
                Executors.newFixedThreadPool(
                        4, // reasonable default for async ops
                        runnable -> {
                            Thread thread = new Thread(runnable, "livedict-async");
                            thread.setDaemon(true);
                            return thread;
                        }
                );
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

    // Core API Set

    /**
     * Stores a key-value pair with no expiry (lives until explicitly deleted
     * or the LiveDict is closed).
     *
     * @param key   the key — must not be null
     * @param value the value to store
     * @throws NullPointerException if key is null
     */
    public void set(K key, V value) {
        set(key, value, config.getDefaultTtlMillis());
    }

    /**
     * Stores a key-value pair with a specific TTL.
     *
     * @param key        the key — must not be null
     * @param value      the value to store
     * @param ttl  how long before this entry expires
     * @param unit    the time unit for {@code ttlAmount}
     * @throws NullPointerException     if key is null
     * @throws IllegalArgumentException if ttlAmount is zero or negative
     */
    public void set(K key, V value, long ttl, TimeUnit unit) {
        if (ttl <= 0) throw new IllegalArgumentException(
                "TTL must be positive. Got: " + ttl
        );
        set(key, value, unit.toMillis(ttl));
    }

    /** Internal set — accepts millis directly. */
    private void set(K key, V value, long ttlMillis) {
        if (key == null) throw new NullPointerException("LiveDict key must not be null");
        LiveDictEntry<V> entry = new LiveDictEntry<>(value, ttlMillis);

        long expiresAt = (ttlMillis > 0)
                ? System.currentTimeMillis() + ttlMillis
                : -1;

        // persist to backend (mode dependent)
        if (writeBehindExecutor != null) {
            // Write-behind: update memory first, enqueue backend write
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

    // Core API Get

    /**
     * Retrieves the value for the given key.
     *
     * <p>Performs a <b>lazy expiry check</b>: if the entry exists but has
     * passed its TTL, it is removed and an empty Optional is returned.
     * This guarantees callers never receive stale values, even if the
     * background reaper hasn't run yet.
     *
     * @param key the key to look up
     * @return an Optional containing the value, or empty if not found / expired
     */
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

    /**
     * Retrieves the value, or returns {@code defaultValue} if not found/expired.
     *
     * <p>Convenience alternative to {@link #get(Object)} for callers that
     * always need a non-null result.
     *
     * @param key          the key to look up
     * @param defaultValue the fallback value
     * @return the stored value, or {@code defaultValue}
     */
    public V getOrDefault(K key, V defaultValue){
        return get(key).orElse(defaultValue);
    }

    // Core API Delete

    /**
     * Explicitly removes a key from the store.
     *
     * @param key the key to remove
     * @return {@code true} if the key existed and was removed, {@code false} if not found
     */
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

    /**
     * Checks whether a key exists and has not expired.
     *
     * @param key the key to check
     * @return {@code true} if the key is present and its TTL has not elapsed
     */
    public boolean exists(K key) {
        return get(key).isPresent(); // lazy expiry check
    }

    /**
     * @return the number of entries currently in the store.
     *         Note: may briefly include entries that have expired but not yet
     *         been reaped. Use {@code exists(key)} to check a specific key accurately.
     */
    public int size() {
        return store.size();
    }

    /**
     * Removes all entries from the store. Does not fire any events.
     */
    public void clear() {
        store.clear();
        // Clear backend (mode-dependent)
        if (writeBehindExecutor != null) {
            // Write-behind: enqueue clear

            writeBehindExecutor.enqueue(
                    new WriteOperation.Clear<>()
            );
        } else {
            // Write-through: clear backend immediately
            try {
                backend.clear();
            } catch (BackendException e) {
                LOGGER.warn("Backend clear failed", e);
            }
        }
    }

    /**
     * @return a read-only snapshot of all keys currently in the store.
     *         May include keys that have expired but not yet been reaped.
     */
    public Set<K> keys() {
        return Collections.unmodifiableSet(store.keySet());
    }

    // ASYNC APIs

    /**
     * Asynchronously stores a key-value pair with no expiry.
     *
     * <p>The operation is submitted to a thread pool and returns immediately.
     * The returned CompletableFuture completes when the operation finishes.
     *
     * <p><b>Write-behind mode:</b> Returns almost instantly (memory update only).
     * <br><b>Write-through mode:</b> Returns when backend write completes.
     *
     * @param key   the key
     * @param value the value
     * @return a CompletableFuture that completes when the operation finishes
     */

    public CompletableFuture<Void> setAsync(K key, V value) {
        return CompletableFuture.runAsync(
                () -> set(key, value),
                asyncExecutor
        );
    }

    /**
     * Asynchronously stores a key-value pair with a TTL.
     *
     * @param key       the key
     * @param value     the value
     * @param ttl the TTL amount
     * @param unit   the TTL unit
     * @return a CompletableFuture that completes when the operation finishes
     */
    public CompletableFuture<Void> setAsync(K key, V value, long ttl, TimeUnit unit) {
        return CompletableFuture.runAsync(
                () -> set(key, value, ttl, unit),
                asyncExecutor
        );
    }

    /**
     * Asynchronously retrieves the value for a given key.
     *
     * @param key the key to look up
     * @return a CompletableFuture containing an Optional with the value,
     *         or empty if not found/expired
     */
    public CompletableFuture<Optional<V>> getAsync(K key) {
        return CompletableFuture.supplyAsync(
                () -> get(key),
                asyncExecutor
        );
    }

    /**
     * Asynchronously retrieves the value or returns a default.
     *
     * @param key          the key to look up
     * @param defaultValue the fallback value
     * @return a CompletableFuture containing the value or default
     */
    public CompletableFuture<V> getOrDefaultAsync(K key, V defaultValue) {
        return CompletableFuture.supplyAsync(
                () -> getOrDefault(key, defaultValue),
                asyncExecutor
        );
    }

    /**
     * Asynchronously deletes a key.
     *
     * @param key the key to remove
     * @return a CompletableFuture containing true if the key existed, false otherwise
     */
    public CompletableFuture<Boolean> deleteAsync(K key) {
        return CompletableFuture.supplyAsync(
                () -> delete(key),
                asyncExecutor
        );
    }

    /**
     * Asynchronously removes all entries from the store.
     *
     * @return a CompletableFuture that completes when the operation finishes
     */
    public CompletableFuture<Void> clearAsync() {
        return CompletableFuture.runAsync(
                this::clear,
                asyncExecutor
        );
    }

    /**
     * Asynchronously checks whether a key exists.
     *
     * @param key the key to check
     * @return a CompletableFuture containing true if the key exists and hasn't expired
     */
    public CompletableFuture<Boolean> existsAsync(K key) {
        return CompletableFuture.supplyAsync(
                () -> exists(key),
                asyncExecutor
        );
    }

    // Event listener APIs

    /**
     * Registers a listener for the given event type.
     *
     * <p>Multiple listeners can be registered for the same event type.
     * They are invoked in registration order.
     *
     * <pre>{@code
     *   cache.on(EventType.ON_EXPIRE, event ->
     *       System.out.println("Key expired: " + event.getKey()));
     * }</pre>
     *
     * @param type     the event to listen for
     * @param listener the callback to invoke — must not be null
     * @throws NullPointerException if type or listener is null
     */
    public void on(EventType type, LiveDictListener<K, V> listener) {
        if (type == null) throw new NullPointerException("EventType must not be null");
        if (listener == null) throw new NullPointerException("Listener must not be null");
        listeners.get(type).add(listener);
    }

    /**
     * Removes a previously registered listener.
     *
     * @param type     the event type the listener was registered under
     * @param listener the exact listener instance to remove
     * @return {@code true} if the listener was found and removed
     */
    public boolean removeListener(EventType type, LiveDictListener<K, V> listener) {
        if (type == null || listener == null) return false;
        return listeners.get(type).remove(listener);
    }

    // internal event dispatch
    /**
     * Fires an event to all registered listeners for the given type.
     *
     * <p>If async listeners are enabled, the event is dispatched to a background
     * thread pool and this method returns immediately. If async is disabled,
     * listeners are invoked synchronously on the calling thread.
     *
     * <p>A misbehaving listener (one that throws) is logged but does not
     * prevent other listeners from running — we always iterate the full list.
     */
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

    /**
     * Core listener invocation logic — extracted so it can run on either
     * the calling thread (sync mode) or the listener thread pool (async mode).
     */
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

    /**
     * Convenience method for expiry events, called by both the lazy-expiry
     * path in {@link #get} and the eager-expiry path in {@link ExpiryScheduler}.
     */
    @Deprecated
    private void fireExpireEvent(K key, V value){
        fireEvent(EventType.ON_EXPIRE, key, value);
    }

    /**
     * Callback invoked by ExpiryScheduler when it reaps an expired entry.
     * Deletes from backend and fires ON_EXPIRE event.
     */
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

    /**
     * Removes an entry from the store and backend, firing the given event.
     *
     * <p>Used by {@code get()} for lazy expiry, and by {@code ExpiryScheduler}
     * for eager expiry.
     */
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

    /**
     * Acquires an exclusive lock on the given key, blocking until available.
     *
     * <p><b>Usage pattern:</b>
     * <pre>{@code
     *   cache.lock("counter");
     *   try {
     *       int val = cache.get("counter").orElse(0);
     *       cache.set("counter", val + 1);
     *   } finally {
     *       cache.unlock("counter");  // MUST unlock in finally
     *   }
     * }</pre>
     *
     * <p><b>Thread safety:</b> The same thread can lock the same key multiple
     * times (reentrant). Each {@code lock()} call must be matched with a
     * corresponding {@code unlock()} call.
     *
     * <p><b>Deadlock risk:</b> If thread A locks key1 then key2, and thread B
     * locks key2 then key1, they will deadlock. Always lock keys in a consistent
     * order, or use {@link #lock(Object, long, TimeUnit)} with a timeout.
     *
     * @param key the key to lock — must not be null
     * @throws NullPointerException if key is null
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    public void lock(K key) throws InterruptedException{
        if (key == null) throw new NullPointerException("Lock key must not be null");
        getLockForKey(key).lockInterruptibly();
    }

    /**
     * Acquires an exclusive lock on the given key, waiting up to the specified time.
     *
     * <p>Returns {@code true} if the lock was acquired, {@code false} if the
     * timeout elapsed before the lock could be acquired.
     *
     * <p><b>Usage pattern:</b>
     * <pre>{@code
     *   if (cache.lock("counter", 500, TimeUnit.MILLISECONDS)) {
     *       try {
     *           // critical section — lock acquired
     *       } finally {
     *           cache.unlock("counter");
     *       }
     *   } else {
     *       // timeout — another thread holds the lock
     *   }
     * }</pre>
     *
     * @param key     the key to lock
     * @param timeout how long to wait for the lock
     * @param unit    the time unit of the timeout
     * @return {@code true} if the lock was acquired, {@code false} on timeout
     * @throws NullPointerException if key is null
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    public boolean lock(K key, long timeout, TimeUnit unit) throws InterruptedException{
        if (key == null) throw new NullPointerException("Key must not be null");
        return getLockForKey(key).tryLock(timeout, unit);
    }

    /**
     * Attempts to acquire an exclusive lock on the given key without waiting.
     *
     * <p>Returns immediately with {@code true} if the lock was available and
     * acquired, or {@code false} if the lock is currently held by another thread.
     *
     * <p>This is a non-blocking alternative to {@link #lock(Object)}.
     *
     * <p><b>Usage pattern:</b>
     * <pre>{@code
     *   if (cache.tryLock("counter")) {
     *       try {
     *           // got the lock immediately
     *       } finally {
     *           cache.unlock("counter");
     *       }
     *   } else {
     *       // lock is busy — skip or retry later
     *   }
     * }</pre>
     *
     * @param key the key to lock
     * @return {@code true} if the lock was acquired, {@code false} otherwise
     * @throws NullPointerException if key is null
     */
    public boolean tryLock(K key) {
        if (key == null) throw new NullPointerException("Key must not be null");
        return getLockForKey(key).tryLock();
    }

    /**
     * Releases the lock on the given key.
     *
     * <p><b>CRITICAL:</b> Must be called by the same thread that acquired the lock.
     * Always call this in a {@code finally} block to ensure it runs even if
     * the critical section throws an exception.
     *
     * <p>If the calling thread does not hold the lock, this throws
     * {@link IllegalMonitorStateException} (standard Java locking contract).
     *
     * @param key the key to unlock
     * @throws NullPointerException        if key is null
     * @throws IllegalMonitorStateException if the current thread does not hold the lock
     */
    public void unlock(K key) {
        if (key == null) throw new NullPointerException("Key must not be null");
        ReentrantLock lock = keyLocks.get(key);
        if (lock == null) throw new IllegalMonitorStateException(
                "Attempt to unlock key: " + key + " which has never been locked"
        );

        lock.unlock();
    }

    /**
     * Checks whether the given key is currently locked by any thread.
     *
     * <p>This is primarily useful for testing and diagnostics. In production
     * code, do not use this for lock-free optimizations — race conditions apply:
     * by the time you check {@code isLocked()}, the state may have changed.
     *
     * @param key the key to check
     * @return {@code true} if the key is locked by any thread
     */
    public boolean isLocked(K key) {
        if (key == null) throw new NullPointerException("Key must not be null");
        ReentrantLock lock = keyLocks.get(key);
        return lock != null && lock.isLocked();
    }

    /**
     * Internal helper: gets or creates the ReentrantLock for a given key.
     *
     * <p>Uses {@link ConcurrentHashMap#computeIfAbsent} which is atomic:
     * if two threads both request a lock for the same key simultaneously,
     * exactly one lock instance is created and returned to both.
     */
    private ReentrantLock getLockForKey(K key) {
        return keyLocks.computeIfAbsent(key, k -> new ReentrantLock());
    }

    // lifecycle
    /**
     * Shuts down the background expiry reaper thread and listener executor.
     *
     * <p>Must be called when the LiveDict is no longer needed to avoid
     * thread leaks. Use try-with-resources to ensure this is always called:
     *
     * <pre>{@code
     *   try (LiveDict<String, User> cache = new LiveDict<>()) {
     *       // use cache ...
     *   }
     * }</pre>
     *
     * <p>If async listeners are enabled, this method waits up to 5 seconds
     * for pending listener tasks to complete before forcing shutdown.
     */
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

        // Stop async API executor
        asyncExecutor.shutdown();
        try {
            if (!asyncExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                LOGGER.warn("Async executor did not terminate cleanly within 5s — forcing shutdown");
                asyncExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            asyncExecutor.shutdownNow();
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
