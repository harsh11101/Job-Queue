package com.github.pants.jobqueue.persistence;

import java.time.Instant;
import java.util.UUID;

/**
 * Immutable projection of a transactional outbox row claimed for publishing.
 *
 * @param id outbox row identifier.
 * @param jobId job aggregate identifier.
 * @param topic destination Kafka topic.
 * @param messageKey Kafka message key.
 * @param payloadJson serialized Kafka value.
 * @param headersJson serialized headers metadata.
 * @param attempts publication attempt count.
 * @param notBefore timestamp before which the row must not be published.
 * @param createdAt creation timestamp.
 */
public record OutboxRecord(
        UUID id,
        UUID jobId,
        String topic,
        String messageKey,
        String payloadJson,
        String headersJson,
        int attempts,
        Instant notBefore,
        Instant createdAt) {
}
