package com.github.pants.jobqueue.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for configuration invariants.
 */
class JobQueuePropertiesTest {

    /**
     * Defaults keep the lock alive for the whole processing window.
     */
    @Test
    void defaultLockTtlCoversProcessingTimeout() {
        assertThat(new JobQueueProperties().isLockTtlCoveringProcessingTimeout()).isTrue();
    }

    /**
     * A lock shorter than the processing timeout is rejected, because it could expire
     * mid-job and let a second worker start the same job.
     */
    @Test
    void lockTtlShorterThanProcessingTimeoutIsInvalid() {
        final JobQueueProperties properties = new JobQueueProperties();
        properties.getWorker().setLockTtl(Duration.ofMinutes(2));
        properties.getWorker().setProcessingTimeout(Duration.ofMinutes(5));

        assertThat(properties.isLockTtlCoveringProcessingTimeout()).isFalse();
    }
}
