package com.github.pants.jobqueue.domain;

import java.time.Duration;

/**
 * Decision returned by the retry policy after a failed job attempt.
 *
 * @param retry true when another retry should be scheduled.
 * @param nextAttempt next attempt number when retrying.
 * @param delay delay before the next attempt is published.
 * @param topic retry topic or DLQ topic selected by the policy.
 */
public record RetryDecision(boolean retry, int nextAttempt, Duration delay, String topic) {
}
