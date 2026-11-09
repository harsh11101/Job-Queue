package com.github.pants.jobqueue.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * Unit tests for Kafka message JSON compatibility.
 */
class JobQueueMessageSerializationTest {

    /**
     * Verifies the job queue message can round-trip through JSON; Jackson 3 handles
     * {@code java.time} types without an extra module.
     */
    @Test
    void jobQueueMessageRoundTripsThroughJson() {
        final ObjectMapper objectMapper = JsonMapper.builder().build();
        final JobQueueMessage message = new JobQueueMessage(
                UUID.randomUUID(), "key-1", 2, Instant.parse("2026-06-06T07:30:00Z"), "trace-1");

        final String json = objectMapper.writeValueAsString(message);
        final JobQueueMessage restored = objectMapper.readValue(json, JobQueueMessage.class);

        assertThat(restored).isEqualTo(message);
    }
}
