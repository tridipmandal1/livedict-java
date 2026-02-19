package com.livedict.reactivity;


@FunctionalInterface
public interface LiveDictListener<K, V> {

    void onEvent(LiveDictEvent<K, V> event);

}
