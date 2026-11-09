package com.github.pants.jobqueue.exception;

import java.util.UUID;

/**
 * Raised when an operator or API caller references an unknown job id.
 */
public class JobNotFoundException extends RuntimeException {

    /**
     * Creates an exception for the missing job id.
     *
     * @param jobId missing job id.
     */
    public JobNotFoundException(final UUID jobId) {
        super("Job not found: " + jobId);
    }

    /**
     * Creates an exception for an idempotency key that matches no job.
     *
     * @param idempotencyKey unknown idempotency key.
     * @return exception instance.
     */
    public static JobNotFoundException forIdempotencyKey(final String idempotencyKey) {
        return new JobNotFoundException("Job not found for idempotency key: " + idempotencyKey);
    }

    private JobNotFoundException(final String message) {
        super(message);
    }
}
