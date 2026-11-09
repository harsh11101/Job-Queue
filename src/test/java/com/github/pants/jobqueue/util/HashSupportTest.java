package com.github.pants.jobqueue.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for SHA-256 hashing helper.
 */
class HashSupportTest {

    /**
     * Verifies deterministic SHA-256 output.
     */
    @Test
    void sha256ReturnsStableLowercaseHexDigest() {
        final HashSupport hashSupport = new HashSupport();

        final String hash = hashSupport.sha256("distributed-job-queue");

        assertThat(hash).hasSize(64);
        assertThat(hash).isEqualTo(hashSupport.sha256("distributed-job-queue"));
        assertThat(hash).matches("[0-9a-f]{64}");
    }
}
