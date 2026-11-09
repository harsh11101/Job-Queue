package com.github.pants.jobqueue.messaging;

import java.time.Instant;
import java.util.UUID;

/**
 * Message sent through Kafka queue and retry topics.
 *
 * @param jobId job aggregate id.
 * @param idempotencyKey business idempotency key used as Kafka key.
 * @param attempt attempt number represented by this message.
 * @param notBefore earliest valid processing time.
 * @param traceId trace id propagated from API to worker logs.
 */
public record JobQueueMessage(
        UUID jobId,
        String idempotencyKey,
        int attempt,
        Instant notBefore,
        String traceId) {
}
