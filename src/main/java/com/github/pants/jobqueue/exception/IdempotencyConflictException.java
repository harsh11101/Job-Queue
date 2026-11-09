package com.github.pants.jobqueue.exception;

import java.io.Serial;

/**
 * Raised when a caller reuses an idempotency key with different request semantics.
 */
public class IdempotencyConflictException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new conflict exception.
     *
     * @param idempotencyKey conflicting key.
     */
    public IdempotencyConflictException(final String idempotencyKey) {
        super("Idempotency key was already used with a different request: " + idempotencyKey);
    }
}
