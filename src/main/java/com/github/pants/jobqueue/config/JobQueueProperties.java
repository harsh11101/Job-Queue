package com.github.pants.jobqueue.config;

import jakarta.validation.constraints.AssertTrue;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Strongly typed configuration for Kafka topics, worker concurrency, retry delays,
 * and transactional outbox publishing behavior.
 */
@Getter
@Setter
@Validated
@ConfigurationProperties(prefix = "app.job-queue")
public class JobQueueProperties {

    /** Kafka client and topic topology settings. */
    private Kafka kafka = new Kafka();

    /** Logical topic names used by the queue. */
    private Topics topics = new Topics();

    /** Worker runtime settings. */
    private Worker worker = new Worker();

    /** Retry policy settings. */
    private Retry retry = new Retry();

    /** Transactional outbox settings. */
    private Outbox outbox = new Outbox();

    /** Browser cross-origin settings for the HTTP API. */
    private Cors cors = new Cors();

    /**
     * A lock that expires while its owner is still processing lets a second worker run
     * the same job, so the lock must outlive the longest expected processing time.
     *
     * @return true when the lock TTL covers the processing timeout.
     */
    @AssertTrue(message = "app.job-queue.worker.lock-ttl must be >= app.job-queue.worker.processing-timeout")
    public boolean isLockTtlCoveringProcessingTimeout() {
        return worker.getLockTtl().compareTo(worker.getProcessingTimeout()) >= 0;
    }

    /** Kafka client and topology parameters. */
    @Getter
    @Setter
    public static class Kafka {
        /** Kafka bootstrap server list. */
        private String bootstrapServers = "localhost:29092";
        /** Consumer group shared by all worker instances. */
        private String consumerGroupId = "distributed-job-queue-workers";
        /** Number of partitions for every application-created topic. */
        private int topicPartitions = 6;
        /** Replication factor for local/prod topic creation. */
        private short topicReplicas = 1;
        /** Maximum Kafka records returned per poll. */
        private int maxPollRecords = 32;
    }

    /** Kafka topic names. */
    @Getter
    @Setter
    public static class Topics {
        /** High-priority queue topic. */
        private String high = "jobs-high";
        /** Low-priority queue topic. */
        private String low = "jobs-low";
        /** First retry topic. */
        private String retry5s = "jobs-retry-5s";
        /** Second retry topic. */
        private String retry30s = "jobs-retry-30s";
        /** Final retry topic before DLQ. */
        private String retry2m = "jobs-retry-2m";
        /** Dead-letter queue topic. */
        private String dlq = "jobs-dlq";
    }

    /** Worker concurrency, lease, and timeout settings. */
    @Getter
    @Setter
    public static class Worker {
        /** Number of concurrent high-priority Kafka listener threads. */
        private int highConcurrency = 6;
        /** Number of concurrent low-priority Kafka listener threads. */
        private int lowConcurrency = 2;
        /** Number of concurrent retry-topic listener threads. */
        private int retryConcurrency = 3;
        /** Redis lease duration used to avoid cross-instance duplicate processing; must be >= processing-timeout. */
        private Duration lockTtl = Duration.ofMinutes(6);
        /**
         * Upper bound on the processing time of one job. Every blocking call inside a job
         * is bounded (JDBC query timeout, Redis command timeout, Kafka delivery timeout), and
         * the lock TTL is validated to cover this value.
         */
        private Duration processingTimeout = Duration.ofMinutes(5);
        /** How often jobs left PROCESSING past their lease are recovered, in milliseconds. */
        private long leaseRecoveryDelayMs = 30_000L;
    }

    /** Retry delay policy. */
    @Getter
    @Setter
    public static class Retry {
        /** Backoff delay after the first failed attempt. */
        private Duration firstDelay = Duration.ofSeconds(5);
        /** Backoff delay after the second failed attempt. */
        private Duration secondDelay = Duration.ofSeconds(30);
        /** Backoff delay for all later retry attempts before DLQ. */
        private Duration thirdDelay = Duration.ofMinutes(2);
        /** Default maximum attempt count for new jobs. */
        private int defaultMaxAttempts = 4;
    }

    /** Transactional outbox scheduler parameters. */
    @Getter
    @Setter
    public static class Outbox {
        /** Enables or disables background publishing from the outbox table. */
        private boolean enabled = true;
        /** Maximum number of due outbox rows claimed per scheduler tick. */
        private int batchSize = 100;
        /** Scheduler fixed delay in milliseconds. */
        private long fixedDelayMs = 1_000L;
        /** Maximum concurrent Kafka publish operations per outbox batch. */
        private int publishConcurrency = 8;
        /**
         * Rows stuck in PUBLISHING for longer than this are reclaimed, because their
         * publisher died mid-batch. Must exceed the Kafka producer delivery timeout (120s).
         */
        private Duration publishingTimeout = Duration.ofMinutes(5);
    }

    /** Cross-origin resource sharing settings. */
    @Getter
    @Setter
    public static class Cors {
        /** Browser origins allowed to call the API, e.g. the dashboard URL. */
        private List<String> allowedOrigins = new ArrayList<>(List.of("http://localhost:5173"));
    }
}
