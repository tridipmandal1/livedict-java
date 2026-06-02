package io.github.tridipmandal1.livedict.backend;


/**
 * Represents an exception that occurs during backend operations.
 *
 * <p>This exception is thrown when there is an error in a backend implementation,
 * such as I/O failures, serialization issues, or any other operational failure
 * within the backend storage.
 *
 * <p>The {@code BackendException} extends {@code RuntimeException}, allowing it
 * to be used for unchecked exceptions. It can wrap an underlying exception to
 * preserve the root cause of the error.
 *
 * @see Backend
 */
public class BackendException extends RuntimeException {
    public BackendException(String message) {
        super(message);
    }
    public BackendException(String message, Exception e) {
        super(message, e);
    }
}
