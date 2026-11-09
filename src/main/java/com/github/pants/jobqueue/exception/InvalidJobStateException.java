package com.github.pants.jobqueue.exception;

import java.io.Serial;

/**
 * Raised when an operation is not valid for the current job state.
 */
public class InvalidJobStateException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new invalid state exception.
     *
     * @param message safe explanation.
     */
    public InvalidJobStateException(final String message) {
        super(message);
    }
}
