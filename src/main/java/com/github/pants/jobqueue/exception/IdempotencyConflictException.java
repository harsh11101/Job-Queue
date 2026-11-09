package com.github.pants.jobqueue.exception;

/**
 * Raised when a caller reuses an idempotency key with different request semantics.
 */
public class IdempotencyConflictException extends RuntimeException {

    /**
     * Creates a new conflict exception.
     *
     * @param idempotencyKey conflicting key.
     */
    public IdempotencyConflictException(final String idempotencyKey) {
        super("Idempotency key was already used with a different request: " + idempotencyKey);
    }
}
