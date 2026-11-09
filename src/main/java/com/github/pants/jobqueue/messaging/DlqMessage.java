package com.github.pants.jobqueue.messaging;

import java.time.Instant;
import java.util.UUID;

/**
 * Message published to the DLQ topic for operations and forensic analysis.
 *
 * @param jobId job aggregate id.
 * @param idempotencyKey original idempotency key.
 * @param attempt final attempt number.
 * @param reason sanitized failure reason.
 * @param failureClass exception class name.
 * @param traceId trace id.
 * @param failedAt failure timestamp.
 */
public record DlqMessage(
        UUID jobId,
        String idempotencyKey,
        int attempt,
        String reason,
        String failureClass,
        String traceId,
        Instant failedAt) {
}
