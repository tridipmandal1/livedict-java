package com.livedict.serialization;

public interface Serializer<T> {

    byte[] serialize(T obj) throws SerializationException;


    T deserialize(byte[] bytes) throws SerializationException;
}
