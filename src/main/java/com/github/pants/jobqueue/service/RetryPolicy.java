package com.github.pants.jobqueue.service;

import com.github.pants.jobqueue.config.JobQueueProperties;
import com.github.pants.jobqueue.domain.RetryDecision;
import com.github.pants.jobqueue.exception.NonRetryableJobException;
import java.time.Duration;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * Classifies failures and selects retry topic/backoff or DLQ.
 */
@Component
@RequiredArgsConstructor
public class RetryPolicy {

    private final JobQueueProperties properties;

    /**
     * Calculates the next failure transition.
     *
     * @param currentAttempt attempt that just failed.
     * @param maxAttempts configured maximum attempts for the job.
     * @param error processing error.
     * @return retry or DLQ decision.
     */
    public RetryDecision decide(final int currentAttempt, final int maxAttempts, final Throwable error) {
        final int nextAttempt = currentAttempt + 1;
        if (error instanceof NonRetryableJobException || nextAttempt > maxAttempts) {
            return new RetryDecision(false, nextAttempt, Duration.ZERO, properties.getTopics().getDlq());
        }
        if (currentAttempt == 1) {
            return new RetryDecision(true, nextAttempt, properties.getRetry().getFirstDelay(), properties.getTopics().getRetry5s());
        }
        if (currentAttempt == 2) {
            return new RetryDecision(true, nextAttempt, properties.getRetry().getSecondDelay(), properties.getTopics().getRetry30s());
        }
        return new RetryDecision(true, nextAttempt, properties.getRetry().getThirdDelay(), properties.getTopics().getRetry2m());
    }
}
