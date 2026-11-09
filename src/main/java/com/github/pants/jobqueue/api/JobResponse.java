package com.github.pants.jobqueue.api;

import com.github.pants.jobqueue.persistence.JobRecord;
import java.time.Instant;
import java.util.UUID;

/**
 * HTTP response that exposes durable job state without leaking internal database details.
 *
 * @param id job identifier.
 * @param idempotencyKey caller-provided idempotency key.
 * @param type job type.
 * @param priority priority lane.
 * @param status durable job status.
 * @param attempt current or last attempt number.
 * @param maxAttempts maximum allowed attempts.
 * @param nextRetryAt next retry time when scheduled.
 * @param payloadJson serialized payload the job was created with.
 * @param resultJson serialized result JSON.
 * @param errorMessage sanitized last error.
 * @param failureClass exception class name of the last failure.
 * @param traceId trace identifier propagated through outbox/Kafka.
 * @param createdAt creation timestamp.
 * @param updatedAt update timestamp.
 * @param completedAt completion timestamp when present.
 */
public record JobResponse(
        UUID id,
        String idempotencyKey,
        String type,
        String priority,
        String status,
        int attempt,
        int maxAttempts,
        Instant nextRetryAt,
        String payloadJson,
        String resultJson,
        String errorMessage,
        String failureClass,
        String traceId,
        Instant createdAt,
        Instant updatedAt,
        Instant completedAt) {

    /**
     * Maps a persistence record to an API response.
     *
     * @param record persisted job record.
     * @return response DTO.
     */
    public static JobResponse from(final JobRecord record) {
        return new JobResponse(
                record.id(),
                record.idempotencyKey(),
                record.type(),
                record.priority(),
                record.status(),
                record.attempt(),
                record.maxAttempts(),
                record.nextRetryAt(),
                record.payloadJson(),
                record.resultJson(),
                record.errorMessage(),
                record.failureClass(),
                record.traceId(),
                record.createdAt(),
                record.updatedAt(),
                record.completedAt());
    }
}
