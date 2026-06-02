package io.github.tridipmandal1.livedict.serialization;

public class SerializationException extends RuntimeException {
    public SerializationException(String message, Exception e) {
        super(message,e);
    }
}
