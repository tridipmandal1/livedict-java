package io.github.tridipmandal1.livedict.reactivity;


/**
 * Callback interface for receiving LiveDict lifecycle events.
 *
 * <p>This is a {@link FunctionalInterface}, which means users can register
 * listeners using a concise lambda expression rather than an anonymous class:
 *
 * <pre>{@code
 *   // Verbose anonymous class (old style — don't do this):
 *   cache.on(EventType.ON_EXPIRE, new LiveDictListener<String, User>() {
 *       public void onEvent(LiveDictEvent<String, User> event) {
 *           System.out.println("Expired: " + event.getKey());
 *       }
 *   });
 *
 *   // Lambda (modern style — do this):
 *   cache.on(EventType.ON_EXPIRE, event -> System.out.println("Expired: " + event.getKey()));
 * }</pre>
 *
 * <p><b>Threading contract:</b> Listeners may be invoked on a thread other
 * than the caller's thread (e.g., the background expiry reaper thread).
 * Implementations must be thread-safe and should not block for long periods,
 * as this delays other listeners and cache operations.
 *
 * @param <K> the type of key in the LiveDict instance
 * @param <V> the type of value in the LiveDict instance
 */
@FunctionalInterface
public interface LiveDictListener<K, V> {

    /**
     * Called when a lifecycle event occurs on a key-value pair.
     *
     * @param event the event payload — never null
     */
    void onEvent(LiveDictEvent<K, V> event);

}
