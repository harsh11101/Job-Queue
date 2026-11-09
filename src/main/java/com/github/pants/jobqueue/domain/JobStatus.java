package com.github.pants.jobqueue.domain;

/**
 * Durable lifecycle states for a job stored in PostgreSQL.
 */
public enum JobStatus {
    /** Job was accepted by the API and stored durably. */
    ACCEPTED,
    /** Job has a due outbox event and is ready to be published to Kafka. */
    QUEUED,
    /** A worker is currently processing the job. */
    PROCESSING,
    /** The last attempt failed and the next retry was durably scheduled. */
    RETRY_SCHEDULED,
    /** Job completed successfully and all side effects are recorded. */
    SUCCEEDED,
    /** Job exhausted retries or failed with a non-retryable error. */
    DLQ,
    /** Cancelled by an operator from QUEUED, RETRY_SCHEDULED or DLQ. Final: never run, retried or replayed. */
    CANCELLED
}
