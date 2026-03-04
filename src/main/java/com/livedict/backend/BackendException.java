package com.livedict.backend;

public class BackendException extends RuntimeException {
    public BackendException(String message, Exception e) {
        super(message, e);
    }
}
