package com.github.pants.jobqueue.exception;

import java.io.Serial;

/**
 * Exception type used by job handlers to signal permanent failures.
 */
public class NonRetryableJobException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Creates a permanent job exception.
     *
     * @param message safe failure message.
     */
    public NonRetryableJobException(final String message) {
        super(message);
    }
}
