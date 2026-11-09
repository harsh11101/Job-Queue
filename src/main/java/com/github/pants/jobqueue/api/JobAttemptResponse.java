package com.github.pants.jobqueue.api;

import com.github.pants.jobqueue.persistence.JobAttemptRecord;
import java.time.Instant;

/**
 * One processing attempt of a job, from the attempt audit table.
 *
 * @param attempt attempt number.
 * @param status attempt outcome, or PROCESSING while running.
 * @param topic Kafka topic the attempt was consumed from.
 * @param errorMessage sanitized failure message.
 * @param failureClass exception class name of the failure.
 * @param startedAt attempt start timestamp.
 * @param finishedAt attempt end timestamp, null while running.
 */
public record JobAttemptResponse(
        int attempt,
        String status,
        String topic,
        String errorMessage,
        String failureClass,
        Instant startedAt,
        Instant finishedAt) {

    /**
     * Maps an attempt audit row to an API response.
     *
     * @param record attempt row.
     * @return response DTO.
     */
    public static JobAttemptResponse from(final JobAttemptRecord record) {
        return new JobAttemptResponse(
                record.attempt(),
                record.status(),
                record.topic(),
                record.errorMessage(),
                record.failureClass(),
                record.startedAt(),
                record.finishedAt());
    }
}
