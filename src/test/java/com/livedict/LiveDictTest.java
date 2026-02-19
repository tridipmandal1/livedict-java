package com.livedict;


import com.livedict.core.LiveDict;
import com.livedict.core.LiveDictConfig;
import com.livedict.reactivity.EventType;
import com.livedict.reactivity.LiveDictListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.annotation.Testable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;


@Testable
public class LiveDictTest {

    private LiveDict<String, String> cache;

    @BeforeEach
    void setUp() {


        LiveDictConfig config =
                LiveDictConfig.builder()
                        .cleanupInterval(500, TimeUnit.MILLISECONDS)
                        .build();
        cache = new LiveDict<>(config);
    }

    @AfterEach
    void reapOff(){
        cache.clear();
    }

    // get / set

    @Test
    @DisplayName("set and get value present")
    void get_existing_key_returns_val(){
        cache.set("name", "bob");
        Optional<String> optional = cache.get("name");
        assertTrue(optional.isPresent(), "Expects value to be true");
        assertEquals("bob", optional.get());
        System.out.println("value: " + optional.get());
    }

    @Test
    @DisplayName("get value not available")
    void get_not_existing(){
        Optional<String> optional =
                cache.get("honey");
        assertFalse(optional.isPresent(), "expect empty optional");
    }

    @Test
    @DisplayName("override value")
    void override_key_value() {
        cache.set("flower", "rose");
        cache.set("flower", "marigold");
        assertEquals("marigold", cache.get("flower").orElseThrow());
    }

    @Test
    @DisplayName("getOrDefaults returns value when present unless default")
    void get_value_or_default(){

        String res = cache.getOrDefault("request", "fallback");
        assertEquals("fallback",res);

    }

    // delete / exists / size

    @Test
    @DisplayName("delete removes the key and returns true")
    void delete_existingKey_removesAndReturnsTrue() {
        cache.set("temp", "data");
        boolean deleted = cache.delete("temp");
        assertTrue(deleted);
        assertFalse(cache.exists("temp"));
    }

    @Test
    @DisplayName("delete on missing key returns false")
    void delete_missingKey_returnsFalse() {
        boolean deleted = cache.delete("nonexistent");
        assertFalse(deleted);
    }

    @Test
    @DisplayName("exists returns false for missing key")
    void exists_missingKey_returnsFalse() {
        assertFalse(cache.exists("nothing"));
    }

    @Test
    @DisplayName("size reflects current entry count")
    void size_afterOperations_reflectsCount() {
        assertEquals(0, cache.size());
        cache.set("a", "1");
        cache.set("b", "2");
        assertEquals(2, cache.size());
        cache.delete("a");
        assertEquals(1, cache.size());
    }

    // ttl expiry



    @Test
    @DisplayName("get returns empty after ttl elapses ie. lazy expiry")
    void get_after_expiry_returns_empty() throws InterruptedException{

        cache.set("key", "value", 300, TimeUnit.MILLISECONDS);
        assertTrue(cache.exists("key"), "value must present before expiry");
        Thread.sleep(500);
        assertFalse(cache.exists("key"), "value should get removed");
    }

    @Test
    @DisplayName("Let reaper clear it ie. eager expiry")
    void wait_to_see_reaper_cleans_it() throws InterruptedException {
        cache.set("key", "value", 100, TimeUnit.MILLISECONDS);
        Thread.sleep(800);
        assertEquals(0, cache.size(), "reaper must have cleaned it");
    }

    @Test
    @DisplayName("not deleting keys having no ttl")
    void keeping_non_ttl_keys() throws InterruptedException {
        cache.set("key", "value");
        Thread.sleep(800);
        assertTrue(cache.exists("key"), "not deleting without ttl");
    }

    @Test
    @DisplayName("set with zero TTL throws IllegalArgumentException")
    void set_withZeroTtl_throwsException() {
        assertThrows(IllegalArgumentException.class,
                () -> cache.set("bad", "value", 0, TimeUnit.SECONDS));
    }

    // callbacks events

    @Test
    @DisplayName("ON_SET listener invoked ")
    void on_set_listener(){

        List<String> captured = new ArrayList<>();
        cache.on(EventType.ON_SET, event -> {captured.add(event.getKey());});
        cache.set("k1", "v1");
        cache.set("k2", "v2");

        assertEquals(List.of("k1","k2"), captured);
    }

    @Test
    @DisplayName("ON_GET listener invoked")
    void on_get_listener() {
        AtomicInteger counter = new AtomicInteger(0);
        cache.on(EventType.ON_GET, event -> counter.incrementAndGet());
        cache.set("x",null);
        cache.get("x");
        cache.get("x");
        cache.get("x");
        cache.get("y");
        cache.get("a");
        cache.get("x");

        assertEquals(4, counter.get());

    }

    @Test
    @DisplayName("ON_EXPIRE listener is invoked on lazy TTL expiry")
    void listener_onExpire_firedOnLazyExpiry() throws InterruptedException {
        List<String> expiredKeys = new ArrayList<>();
        cache.on(EventType.ON_EXPIRE, event -> expiredKeys.add(event.getKey()));

        cache.set("session", "data", 200, TimeUnit.MILLISECONDS);
        Thread.sleep(400);

        // Trigger lazy expiry by calling get()
        cache.get("session");

        assertTrue(expiredKeys.contains("session"), "ON_EXPIRE should fire on lazy expiry");
    }

    @Test
    @DisplayName("ON_DELETE listener is invoked on delete()")
    void listener_onDelete_isInvoked() {
        List<String> deletedKeys = new ArrayList<>();
        cache.on(EventType.ON_DELETE, event -> deletedKeys.add(event.getKey()));

        cache.set("goodbye", "value");
        cache.delete("goodbye");

        assertEquals(List.of("goodbye"), deletedKeys);
    }

    @Test
    @DisplayName("multiple listeners for the same event all get invoked")
    void listener_multiple_allInvoked() {
        AtomicInteger counter = new AtomicInteger();
        cache.on(EventType.ON_SET, e -> counter.incrementAndGet());
        cache.on(EventType.ON_SET, e -> counter.incrementAndGet());
        cache.on(EventType.ON_SET, e -> counter.incrementAndGet());

        cache.set("trigger", "it");

        assertEquals(3, counter.get(), "All three listeners should be called");
    }

    @Test
    @DisplayName("a throwing listener does not prevent other listeners from running")
    void listener_throwingListener_doesNotBlockOthers() {
        AtomicInteger counter = new AtomicInteger();

        // First listener: throws
        cache.on(EventType.ON_SET, e -> { throw new RuntimeException("I misbehave!"); });
        // Second listener: should still run
        cache.on(EventType.ON_SET, e -> counter.incrementAndGet());

        assertDoesNotThrow(() -> cache.set("key", "val"));
        assertEquals(1, counter.get(), "Second listener should still run despite first throwing");
    }

    @Test
    @DisplayName("removeListener stops listener from being invoked")
    void removeListener_removedListener_notInvoked() {
        List<String> seen = new ArrayList<>();

        // Hold a reference to the listener instance so we can remove it later
        LiveDictListener<String, String> myListener = e -> seen.add(e.getKey());
        cache.on(EventType.ON_SET, myListener);

        cache.set("before", "remove");
        assertTrue(seen.contains("before"), "Listener should fire before removal");

        cache.removeListener(EventType.ON_SET, myListener);
        cache.set("after", "remove");

        assertFalse(seen.contains("after"), "Listener should not fire after removal");
    }
}
