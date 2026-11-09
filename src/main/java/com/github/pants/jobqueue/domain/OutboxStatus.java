package com.github.pants.jobqueue.domain;

/**
 * Publication state for transactional outbox rows.
 */
public enum OutboxStatus {
    /** Row is due for publishing once its not-before timestamp is reached. */
    PENDING,
    /** Row is claimed by a publisher instance. */
    PUBLISHING,
    /** Row was successfully sent to Kafka. */
    PUBLISHED,
    /** Row publication failed and will be retried by the scheduler. */
    FAILED,
    /** Row's job was cancelled before it was sent; it is never published. */
    CANCELLED
}
