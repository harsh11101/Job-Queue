package com.github.pants.jobqueue.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.pants.jobqueue.config.JobQueueProperties;
import com.github.pants.jobqueue.domain.RetryDecision;
import com.github.pants.jobqueue.exception.NonRetryableJobException;
import com.github.pants.jobqueue.exception.RetryableJobException;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for retry topic/backoff decisions.
 */
class RetryPolicyTest {

    /**
     * Verifies first retry goes to the first backoff topic.
     */
    @Test
    void firstRetryUsesFiveSecondBackoffTopic() {
        final JobQueueProperties properties = new JobQueueProperties();
        final RetryPolicy policy = new RetryPolicy(properties);

        final RetryDecision decision = policy.decide(1, 4, new RetryableJobException("temporary"));

        assertThat(decision.retry()).isTrue();
        assertThat(decision.nextAttempt()).isEqualTo(2);
        assertThat(decision.delay()).isEqualTo(Duration.ofSeconds(5));
        assertThat(decision.topic()).isEqualTo("jobs-retry-5s");
    }

    /**
     * Verifies non-retryable errors go directly to DLQ.
     */
    @Test
    void nonRetryableErrorGoesDirectlyToDlq() {
        final RetryPolicy policy = new RetryPolicy(new JobQueueProperties());

        final RetryDecision decision = policy.decide(1, 4, new NonRetryableJobException("bad request"));

        assertThat(decision.retry()).isFalse();
        assertThat(decision.topic()).isEqualTo("jobs-dlq");
    }

    /**
     * Verifies attempts above the configured maximum go to DLQ.
     */
    @Test
    void exhaustedAttemptsGoToDlq() {
        final RetryPolicy policy = new RetryPolicy(new JobQueueProperties());

        final RetryDecision decision = policy.decide(4, 4, new RetryableJobException("still broken"));

        assertThat(decision.retry()).isFalse();
        assertThat(decision.topic()).isEqualTo("jobs-dlq");
    }
}
