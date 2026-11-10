package com.demp.platform.messaging;

/** Poison message: retrying will not help, the record goes straight to the dead letter topic. */
public class InvalidMessageException extends RuntimeException {

    public InvalidMessageException(String message) {
        super(message);
    }

    public InvalidMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}
