package com.livedict.serialization;

public class SerializationException extends RuntimeException {
    public SerializationException(String message, Exception e) {
        super(message,e);
    }
}
