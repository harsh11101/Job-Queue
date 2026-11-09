package com.github.pants.jobqueue.persistence;

import java.time.Instant;
import java.util.UUID;

/**
 * Immutable projection of the jobs table.
 *
 * @param id job identifier.
 * @param idempotencyKey caller idempotency key.
 * @param requestHash deterministic hash of create request semantics.
 * @param type job type.
 * @param priority priority lane.
 * @param status durable status.
 * @param payloadJson serialized JSON payload.
 * @param resultJson serialized JSON result.
 * @param errorMessage sanitized last error message.
 * @param failureClass exception class name of the last failure.
 * @param traceId trace identifier.
 * @param attempt current or latest attempt number.
 * @param maxAttempts maximum allowed attempts.
 * @param nextRetryAt next retry timestamp.
 * @param createdAt creation timestamp.
 * @param updatedAt update timestamp.
 * @param completedAt completion timestamp.
 */
public record JobRecord(
        UUID id,
        String idempotencyKey,
        String requestHash,
        String type,
        String priority,
        String status,
        String payloadJson,
        String resultJson,
        String errorMessage,
        String failureClass,
        String traceId,
        int attempt,
        int maxAttempts,
        Instant nextRetryAt,
        Instant createdAt,
        Instant updatedAt,
        Instant completedAt) {
}
