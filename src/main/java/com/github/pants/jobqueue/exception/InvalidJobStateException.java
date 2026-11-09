package com.github.pants.jobqueue.exception;

/**
 * Raised when an operation is not valid for the current job state.
 */
public class InvalidJobStateException extends RuntimeException {

    /**
     * Creates a new invalid state exception.
     *
     * @param message safe explanation.
     */
    public InvalidJobStateException(final String message) {
        super(message);
    }
}
