package com.github.pants.jobqueue.service;

import com.github.pants.jobqueue.config.JobQueueProperties;
import com.github.pants.jobqueue.domain.JobExecutionResult;
import com.github.pants.jobqueue.domain.JobStatus;
import com.github.pants.jobqueue.domain.RetryDecision;
import com.github.pants.jobqueue.exception.RetryableJobException;
import com.github.pants.jobqueue.messaging.DlqMessage;
import com.github.pants.jobqueue.messaging.JobQueueMessage;
import com.github.pants.jobqueue.persistence.JobRecord;
import com.github.pants.jobqueue.persistence.JobStore;
import com.github.pants.jobqueue.persistence.NewOutboxEvent;
import com.github.pants.jobqueue.util.JsonSupport;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Worker orchestration service that processes Kafka job messages safely.
 *
 * <p>Kafka provides at-least-once delivery. This service provides exactly-once
 * business behavior by combining Redis leases, durable job state, and unique
 * side-effect receipts.</p>
 *
 * <p>Processing is plain blocking code. It runs on the Kafka listener's virtual
 * thread, so waiting on PostgreSQL or Redis parks the virtual thread instead of
 * occupying a platform thread.</p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class JobWorkerService {

    /** Metric tag for attempts ended by lease recovery rather than by a Kafka message. */
    static final String LEASE_RECOVERY = "lease-recovery";

    /** States a job never leaves on its own; messages for them are acknowledged and skipped. */
    private static final Set<String> TERMINAL =
            Set.of(JobStatus.SUCCEEDED.name(), JobStatus.DLQ.name(), JobStatus.CANCELLED.name());

    private final Clock clock;
    private final DistributedLockService lockService;
    private final JobExecutor jobExecutor;
    private final JobQueueProperties properties;
    private final JobStore jobStore;
    private final JsonSupport jsonSupport;
    private final MeterRegistry meterRegistry;
    private final RetryPolicy retryPolicy;
    private final TransactionTemplate transactionTemplate;
    private final WorkerIdentity workerIdentity;

    /**
     * Processes one serialized Kafka message. Returning normally means a durable
     * state transition (or a safe skip) happened and the message may be acknowledged;
     * an exception means it must be redelivered.
     *
     * @param rawMessage serialized {@link JobQueueMessage}.
     * @param topic Kafka topic from which the message was consumed.
     */
    public void process(final String rawMessage, final String topic) {
        final JobQueueMessage message = jsonSupport.read(rawMessage, JobQueueMessage.class);
        final String lockKey = "jobqueue:job-lock:" + message.jobId();
        final boolean acquired = lockService.withLock(
                lockKey, properties.getWorker().getLockTtl(), () -> processLocked(message, topic));
        if (!acquired) {
            meterRegistry.counter("jobqueue.worker.lock.skipped", "topic", topic).increment();
            log.info("Job {} is already leased by another worker", message.jobId());
        }
    }

    private void processLocked(final JobQueueMessage message, final String topic) {
        final Optional<JobRecord> found = jobStore.findJobById(message.jobId());
        if (found.isEmpty()) {
            log.warn("Ignoring message for missing job {}", message.jobId());
            return;
        }
        final JobRecord job = found.get();
        if (TERMINAL.contains(job.status())) {
            meterRegistry.counter("jobqueue.worker.terminal.skipped", "status", job.status()).increment();
            return;
        }
        final Instant now = Instant.now(clock);
        if (message.notBefore() != null && now.isBefore(message.notBefore())) {
            rescheduleEarlyMessage(message, topic, message.notBefore());
            return;
        }
        executeAttempt(job, message, topic);
    }

    private void executeAttempt(final JobRecord job, final JobQueueMessage message, final String topic) {
        try {
            final Instant startedAt = Instant.now(clock);
            final Instant leaseUntil = startedAt.plus(properties.getWorker().getLockTtl());
            if (!jobStore.markProcessing(job.id(), message.attempt(), workerIdentity.value(), startedAt, leaseUntil)) {
                // Cancelled since the read above, or a stale message for an attempt that already ended.
                meterRegistry.counter("jobqueue.worker.start.skipped", "topic", topic).increment();
                log.info("Job {} cannot start attempt {}; message skipped", job.id(), message.attempt());
                return;
            }
            jobStore.insertAttemptStarted(job.id(), message.attempt(), topic, startedAt);
            final JobExecutionResult result = jobExecutor.execute(job);
            final boolean recorded = Boolean.TRUE.equals(transactionTemplate.execute(status -> {
                final Instant finishedAt = Instant.now(clock);
                if (!jobStore.markSucceeded(job.id(), message.attempt(), result.resultJson(), finishedAt)) {
                    return false;
                }
                jobStore.finishAttempt(
                        job.id(), message.attempt(), JobStatus.SUCCEEDED.name(), null, null, finishedAt);
                return true;
            }));
            if (!recorded) {
                leaseLost(job, message.attempt(), topic);
                return;
            }
        } catch (RuntimeException error) {
            handleFailure(job, message.attempt(), topic, error);
            return;
        }
        meterRegistry.counter("jobqueue.worker.completed", "topic", topic).increment();
    }

    /**
     * Ends an attempt whose worker lease expired while the job was still {@code PROCESSING}.
     * The attempt counts as a retryable failure, so the job gets its normal retry or goes
     * to DLQ when it has no attempts left. If the original worker is in fact still running,
     * its own result is fenced out (see {@link JobStore}); the side-effect receipt stops the
     * retry from repeating an effect that worker already produced.
     *
     * @param job job found {@code PROCESSING} with an expired lease.
     */
    public void recoverExpiredLease(final JobRecord job) {
        final boolean recovered = handleFailure(job, job.attempt(), LEASE_RECOVERY,
                new RetryableJobException("Worker lease expired before the attempt finished"));
        if (recovered) {
            meterRegistry.counter("jobqueue.worker.lease.recovered").increment();
            log.warn("Recovered job {} attempt {}: its worker lease expired", job.id(), job.attempt());
        }
    }

    private void leaseLost(final JobRecord job, final int attempt, final String topic) {
        meterRegistry.counter("jobqueue.worker.lease.lost", "topic", topic).increment();
        log.warn("Job {} attempt {} finished after its lease was recovered; result discarded", job.id(), attempt);
    }

    /**
     * Records a failed attempt and schedules its retry or DLQ message in one transaction.
     *
     * @return true when this call ended the attempt, false when the attempt no longer owned the job.
     */
    private boolean handleFailure(
            final JobRecord job,
            final int attempt,
            final String topic,
            final RuntimeException error) {
        final Instant now = Instant.now(clock);
        final String safeError = safeMessage(error);
        final String failureClass = error.getClass().getName();
        final RetryDecision decision = retryPolicy.decide(attempt, job.maxAttempts(), error);
        if (decision.retry()) {
            final Instant retryAt = now.plus(decision.delay());
            final JobQueueMessage retryMessage = new JobQueueMessage(
                    job.id(), job.idempotencyKey(), decision.nextAttempt(), retryAt, job.traceId());
            final NewOutboxEvent retryOutbox = new NewOutboxEvent(
                    UUID.randomUUID(),
                    job.id(),
                    decision.topic(),
                    job.idempotencyKey(),
                    jsonSupport.write(retryMessage),
                    "{}",
                    retryAt,
                    now);
            final boolean scheduled = Boolean.TRUE.equals(transactionTemplate.execute(status -> {
                if (!jobStore.markRetryScheduled(job.id(), attempt, safeError, failureClass, retryAt, now)) {
                    return false;
                }
                jobStore.finishAttempt(
                        job.id(), attempt, JobStatus.RETRY_SCHEDULED.name(), safeError, failureClass, now);
                jobStore.insertOutboxEvent(retryOutbox);
                return true;
            }));
            if (!scheduled) {
                leaseLost(job, attempt, topic);
                return false;
            }
            meterRegistry.counter("jobqueue.worker.retry.scheduled", "topic", topic).increment();
            return true;
        }

        final DlqMessage dlqMessage = new DlqMessage(
                job.id(), job.idempotencyKey(), attempt, safeError, failureClass, job.traceId(), now);
        final NewOutboxEvent dlqOutbox = new NewOutboxEvent(
                UUID.randomUUID(),
                job.id(),
                properties.getTopics().getDlq(),
                job.idempotencyKey(),
                jsonSupport.write(dlqMessage),
                "{}",
                now,
                now);
        final boolean deadLettered = Boolean.TRUE.equals(transactionTemplate.execute(status -> {
            if (!jobStore.markDlq(job.id(), attempt, safeError, failureClass, now)) {
                return false;
            }
            jobStore.finishAttempt(job.id(), attempt, JobStatus.DLQ.name(), safeError, failureClass, now);
            jobStore.insertOutboxEvent(dlqOutbox);
            return true;
        }));
        if (!deadLettered) {
            leaseLost(job, attempt, topic);
            return false;
        }
        meterRegistry.counter("jobqueue.worker.dlq", "topic", topic).increment();
        return true;
    }

    private void rescheduleEarlyMessage(final JobQueueMessage message, final String topic, final Instant notBefore) {
        final Instant now = Instant.now(clock);
        jobStore.insertOutboxEvent(new NewOutboxEvent(
                UUID.randomUUID(),
                message.jobId(),
                topic,
                message.idempotencyKey(),
                jsonSupport.write(message),
                "{}",
                notBefore,
                now));
        meterRegistry.counter("jobqueue.worker.early.rescheduled", "topic", topic).increment();
    }

    private String safeMessage(final Throwable error) {
        final String message = error.getMessage();
        final String value = message == null || message.isBlank() ? error.getClass().getSimpleName() : message;
        return value.length() > 1_000 ? value.substring(0, 1_000) : value;
    }
}
