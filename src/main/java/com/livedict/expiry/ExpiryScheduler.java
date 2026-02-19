package com.livedict.expiry;

import com.livedict.core.LiveDictEntry;

import java.util.Map;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ExpiryScheduler<K,V> {

    private static final Logger LOGGER =
            Logger.getLogger(ExpiryScheduler.class.getName());

    private final ScheduledExecutorService scheduler;

    private ScheduledFuture<?> scheduledTask;

    private final ConcurrentHashMap<K, LiveDictEntry<V>> store;
    private final BiConsumer<K, V> onExpireCallback;

    public ExpiryScheduler(ConcurrentHashMap<K, LiveDictEntry<V>> store,
                           BiConsumer<K, V> onExpireCallback) {
        this.store = store;
        this.onExpireCallback = onExpireCallback;

        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable ->{
            Thread thread = new Thread(runnable, "livedict-expiry-reaper");
            thread.setDaemon(true);
            return thread;
        });
    }

    public void start(long intervalMillis) {
        scheduledTask = scheduler.scheduleAtFixedRate(
            this::reapExpiredEntries,
                intervalMillis,
                intervalMillis,
                TimeUnit.MILLISECONDS
        );
    }

    public void stop() {
        if (scheduledTask != null) {
            scheduledTask.cancel(false);
        }

        scheduler.shutdown();
        LOGGER.fine("LiveDict expiry reaper stopped.");
    }

    private void reapExpiredEntries() {
        try {
            for(Map.Entry<K, LiveDictEntry<V>> entry: store.entrySet()) {
                LiveDictEntry<V> dictEntry = entry.getValue();

                if (dictEntry.isExpired()) {
                    K key = entry.getKey();
                    boolean removed = store.remove(key, dictEntry);
                    if (removed) {
                        onExpireCallback.accept(key, dictEntry.getValue());
                    }
                }
            }
        } catch (Throwable t) {
            LOGGER.log(Level.SEVERE, "Throwable caught during Livedict expiry reaping", t);
        }
    }
}
