package com.github.pants.jobqueue.persistence;

import java.time.Instant;
import java.util.UUID;

/**
 * Command object used to insert a transactional outbox event.
 *
 * @param id outbox event id.
 * @param jobId job id.
 * @param topic Kafka topic.
 * @param messageKey Kafka key.
 * @param payloadJson Kafka message payload.
 * @param headersJson serialized headers.
 * @param notBefore publication due timestamp.
 * @param now creation timestamp.
 */
public record NewOutboxEvent(
        UUID id,
        UUID jobId,
        String topic,
        String messageKey,
        String payloadJson,
        String headersJson,
        Instant notBefore,
        Instant now) {
}
