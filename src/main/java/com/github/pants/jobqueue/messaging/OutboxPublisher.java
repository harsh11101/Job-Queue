package com.github.pants.jobqueue.messaging;

import com.github.pants.jobqueue.config.JobQueueProperties;
import com.github.pants.jobqueue.persistence.JobStore;
import com.github.pants.jobqueue.persistence.OutboxRecord;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Background publisher that drains due rows from the transactional outbox and sends
 * them to Kafka.
 *
 * <p>Rows are first claimed in PostgreSQL with {@code FOR UPDATE SKIP LOCKED}; this
 * lets many application instances publish concurrently without double-publishing the
 * same outbox row.</p>
 *
 * <p>Each scheduler tick blocks until its batch is finished. A fixed-delay schedule
 * never starts the next tick before the previous one returns, so ticks cannot overlap.
 * Inside a tick every row is published on its own virtual thread, and a semaphore keeps
 * at most {@code publish-concurrency} rows in flight so the batch never holds more
 * database connections than that.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxPublisher {

    private final Clock clock;
    private final JobQueueProperties properties;
    private final JobStore jobStore;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final MeterRegistry meterRegistry;

    /**
     * Publishes a batch of due outbox rows on a fixed schedule.
     */
    @Scheduled(fixedDelayString = "${app.job-queue.outbox.fixed-delay-ms:1000}")
    public void publishDueRows() {
        if (!properties.getOutbox().isEnabled()) {
            return;
        }
        try {
            final Instant now = Instant.now(clock);
            final List<OutboxRecord> batch = jobStore.claimDueOutbox(
                    now,
                    now.minus(properties.getOutbox().getPublishingTimeout()),
                    properties.getOutbox().getBatchSize());
            publishAll(batch);
        } catch (RuntimeException exception) {
            log.error("Outbox publisher batch failed", exception);
        }
    }

    private void publishAll(final List<OutboxRecord> batch) {
        if (batch.isEmpty()) {
            return;
        }
        final Semaphore permits = new Semaphore(properties.getOutbox().getPublishConcurrency());
        // Closing the executor waits for every submitted task to finish.
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (final OutboxRecord record : batch) {
                executor.submit(() -> {
                    permits.acquireUninterruptibly();
                    try {
                        publishOne(record);
                    } finally {
                        permits.release();
                    }
                });
            }
        }
    }

    private void publishOne(final OutboxRecord record) {
        try {
            kafkaTemplate.send(record.topic(), record.messageKey(), record.payloadJson()).get();
            jobStore.markOutboxPublished(record.id(), Instant.now(clock));
            meterRegistry.counter("jobqueue.outbox.published", "topic", record.topic()).increment();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            markFailed(record, exception);
        } catch (ExecutionException exception) {
            markFailed(record, exception.getCause() == null ? exception : exception.getCause());
        } catch (RuntimeException exception) {
            markFailed(record, exception);
        }
    }

    private void markFailed(final OutboxRecord record, final Throwable error) {
        meterRegistry.counter("jobqueue.outbox.publish.failed", "topic", record.topic()).increment();
        try {
            jobStore.markOutboxFailed(record.id(), safeMessage(error), Instant.now(clock));
        } catch (RuntimeException exception) {
            // The row stays PUBLISHING and is reclaimed after the publishing timeout.
            log.error("Could not mark outbox row {} as failed", record.id(), exception);
        }
    }

    private String safeMessage(final Throwable error) {
        final String message = error.getMessage();
        if (message == null || message.isBlank()) {
            return error.getClass().getSimpleName();
        }
        return message.length() > 1_000 ? message.substring(0, 1_000) : message;
    }
}
