package com.github.pants.jobqueue;

import com.github.pants.jobqueue.config.JobQueueProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Application entry point for the distributed priority job queue service.
 *
 * <p>The application exposes a blocking Spring MVC HTTP API served on virtual threads,
 * persists job state in PostgreSQL over JDBC, publishes queue messages through a
 * transactional outbox, and consumes Kafka topics with manual acknowledgements to
 * preserve at-least-once delivery semantics.</p>
 */
@EnableKafka
@EnableScheduling
@SpringBootApplication
@EnableConfigurationProperties(JobQueueProperties.class)
public class DistributedPriorityJobQueueApplication {

    /**
     * Starts the Spring Boot application.
     *
     * @param args command-line arguments passed by the runtime.
     */
    public static void main(final String[] args) {
        SpringApplication.run(DistributedPriorityJobQueueApplication.class, args);
    }
}
