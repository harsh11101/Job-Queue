package com.github.pants.jobqueue.persistence;

import java.time.Instant;
import java.util.UUID;

/**
 * Command object containing normalized values for a new persisted job.
 *
 * @param id generated job id.
 * @param idempotencyKey normalized idempotency key.
 * @param requestHash deterministic hash for conflict detection.
 * @param type normalized job type.
 * @param priority normalized priority.
 * @param payloadJson serialized payload.
 * @param maxAttempts maximum attempts.
 * @param traceId trace id.
 * @param now creation timestamp.
 */
public record NewJob(
        UUID id,
        String idempotencyKey,
        String requestHash,
        String type,
        String priority,
        String payloadJson,
        int maxAttempts,
        String traceId,
        Instant now) {
}
