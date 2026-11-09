package com.github.pants.jobqueue.domain;

/**
 * Worker error classification used by the retry policy.
 */
public enum FailureClassification {
    /** Error can be retried safely. */
    RETRYABLE,
    /** Error should not be retried and must go directly to DLQ. */
    NON_RETRYABLE
}
