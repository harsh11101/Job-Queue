package com.github.pants.jobqueue.exception;

/**
 * Exception type used by job handlers to signal safe retryable failures.
 */
public class RetryableJobException extends RuntimeException {

    /**
     * Creates a retryable job exception.
     *
     * @param message safe failure message.
     */
    public RetryableJobException(final String message) {
        super(message);
    }
}
