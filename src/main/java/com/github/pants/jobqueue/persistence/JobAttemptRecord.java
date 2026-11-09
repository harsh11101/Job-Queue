package com.github.pants.jobqueue.persistence;

import java.time.Instant;

/**
 * Immutable projection of one {@code job_attempts} audit row.
 *
 * @param attempt attempt number.
 * @param status attempt outcome, or PROCESSING while running.
 * @param topic Kafka topic the attempt was consumed from.
 * @param errorMessage sanitized failure message.
 * @param failureClass exception class name of the failure.
 * @param startedAt attempt start timestamp.
 * @param finishedAt attempt end timestamp, null while running.
 */
public record JobAttemptRecord(
        int attempt,
        String status,
        String topic,
        String errorMessage,
        String failureClass,
        Instant startedAt,
        Instant finishedAt) {
}
