package com.github.pants.jobqueue.domain;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for public priority names used in API and persistence.
 */
class JobPriorityTest {

    /**
     * Verifies enum names remain stable because they are stored as strings.
     */
    @Test
    void priorityNamesAreStable() {
        assertThat(JobPriority.HIGH.name()).isEqualTo("HIGH");
        assertThat(JobPriority.LOW.name()).isEqualTo("LOW");
    }
}
