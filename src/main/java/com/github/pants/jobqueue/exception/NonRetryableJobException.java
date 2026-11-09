package com.github.pants.jobqueue.exception;

/**
 * Exception type used by job handlers to signal permanent failures.
 */
public class NonRetryableJobException extends RuntimeException {

    /**
     * Creates a permanent job exception.
     *
     * @param message safe failure message.
     */
    public NonRetryableJobException(final String message) {
        super(message);
    }
}
