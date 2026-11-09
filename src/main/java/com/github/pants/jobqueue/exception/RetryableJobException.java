package com.github.pants.jobqueue.exception;

import java.io.Serial;

/**
 * Exception type used by job handlers to signal safe retryable failures.
 */
public class RetryableJobException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Creates a retryable job exception.
     *
     * @param message safe failure message.
     */
    public RetryableJobException(final String message) {
        super(message);
    }
}
