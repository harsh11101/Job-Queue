package com.github.pants.jobqueue.service;

import com.github.pants.jobqueue.api.CreateJobRequest;
import com.github.pants.jobqueue.api.CreateJobResult;
import com.github.pants.jobqueue.api.JobAttemptResponse;
import com.github.pants.jobqueue.api.JobPageResponse;
import com.github.pants.jobqueue.api.JobResponse;
import com.github.pants.jobqueue.api.JobStatsResponse;
import com.github.pants.jobqueue.config.JobQueueProperties;
import com.github.pants.jobqueue.domain.JobPriority;
import com.github.pants.jobqueue.domain.JobStatus;
import com.github.pants.jobqueue.exception.IdempotencyConflictException;
import com.github.pants.jobqueue.exception.InvalidJobStateException;
import com.github.pants.jobqueue.exception.JobNotFoundException;
import com.github.pants.jobqueue.messaging.JobQueueMessage;
import com.github.pants.jobqueue.persistence.JobPage;
import com.github.pants.jobqueue.persistence.JobRecord;
import com.github.pants.jobqueue.persistence.JobSearchFilter;
import com.github.pants.jobqueue.persistence.JobStore;
import com.github.pants.jobqueue.persistence.NewJob;
import com.github.pants.jobqueue.persistence.NewOutboxEvent;
import com.github.pants.jobqueue.util.HashSupport;
import com.github.pants.jobqueue.util.JsonSupport;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.OptionalDouble;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Application service for API-level job commands and queries.
 */
@Service
@RequiredArgsConstructor
public class JobCommandService {

    private final Clock clock;
    private final HashSupport hashSupport;
    private final JobQueueProperties properties;
    private final JobStore jobStore;
    private final JsonSupport jsonSupport;
    private final MeterRegistry meterRegistry;
    private final TransactionTemplate transactionTemplate;

    /**
     * Creates a new job or returns the existing job for an idempotent duplicate request.
     *
     * <p>The job row and its first outbox event commit in one transaction. Concurrent
     * requests with the same key are serialized by the unique index: the loser's
     * {@code ON CONFLICT DO NOTHING} waits for the winner to commit, then reads the
     * winner's row.</p>
     *
     * @param request create-job request.
     * @return create result containing job response and created flag.
     */
    public CreateJobResult create(final CreateJobRequest request) {
        final Instant now = Instant.now(clock);
        final String idempotencyKey = normalize(request.idempotencyKey());
        final String type = normalizeType(request.type());
        final String payloadJson = jsonSupport.canonical(request.payload());
        final String requestHash = requestHash(idempotencyKey, type, request.priority(), payloadJson);
        final int maxAttempts = request.maxAttempts() == null
                ? properties.getRetry().getDefaultMaxAttempts()
                : request.maxAttempts();
        final UUID jobId = UUID.randomUUID();
        final String traceId = UUID.randomUUID().toString();
        final NewJob job = new NewJob(
                jobId,
                idempotencyKey,
                requestHash,
                type,
                request.priority().name(),
                payloadJson,
                maxAttempts,
                traceId,
                now);
        final String topic = topicForPriority(request.priority());
        final JobQueueMessage message = new JobQueueMessage(jobId, idempotencyKey, 1, now, traceId);
        final NewOutboxEvent outbox = outbox(jobId, idempotencyKey, topic, jsonSupport.write(message), now);

        final CreateJobResult result = transactionTemplate.execute(status -> {
            if (jobStore.insertJobIfAbsent(job)) {
                jobStore.insertOutboxEvent(outbox);
                return new CreateJobResult(JobResponse.from(requireJob(jobId)), true);
            }
            final JobRecord existing = jobStore.findJobByIdempotencyKey(idempotencyKey)
                    .orElseThrow(() -> JobNotFoundException.forIdempotencyKey(idempotencyKey));
            ensureSameRequest(existing, requestHash);
            return new CreateJobResult(JobResponse.from(existing), false);
        });
        meterRegistry.counter("jobqueue.jobs.created", "created", Boolean.toString(result.created())).increment();
        return result;
    }

    /**
     * Returns a job by id.
     *
     * @param jobId job id.
     * @return job response.
     */
    public JobResponse get(final UUID jobId) {
        return JobResponse.from(requireJob(jobId));
    }

    /**
     * Returns a job by idempotency key.
     *
     * @param idempotencyKey idempotency key.
     * @return job response.
     */
    public JobResponse getByIdempotencyKey(final String idempotencyKey) {
        final String key = normalize(idempotencyKey);
        return jobStore.findJobByIdempotencyKey(key)
                .map(JobResponse::from)
                .orElseThrow(() -> JobNotFoundException.forIdempotencyKey(key));
    }

    /**
     * Lists jobs matching optional filters, newest first.
     *
     * @param status status filter, may be null.
     * @param priority priority filter, may be null.
     * @param type job type filter (case-insensitive), may be null or blank.
     * @param createdFrom inclusive creation lower bound, may be null.
     * @param createdTo exclusive creation upper bound, may be null.
     * @param page zero-based page index.
     * @param size page size.
     * @return page of jobs.
     */
    public JobPageResponse list(
            final JobStatus status,
            final JobPriority priority,
            final String type,
            final Instant createdFrom,
            final Instant createdTo,
            final int page,
            final int size) {
        final JobSearchFilter filter = new JobSearchFilter(
                status == null ? null : status.name(),
                priority == null ? null : priority.name(),
                type == null || type.isBlank() ? null : normalizeType(type),
                createdFrom,
                createdTo);
        final JobPage result = jobStore.searchJobs(filter, page, size);
        final int totalPages = (int) ((result.totalElements() + size - 1) / size);
        return new JobPageResponse(
                result.items().stream().map(JobResponse::from).toList(),
                page,
                size,
                result.totalElements(),
                totalPages);
    }

    /**
     * Returns the attempt history of a job, oldest first.
     *
     * @param jobId job id.
     * @return attempts.
     */
    public List<JobAttemptResponse> attempts(final UUID jobId) {
        requireJob(jobId);
        return jobStore.findAttempts(jobId).stream().map(JobAttemptResponse::from).toList();
    }

    /**
     * Replays a DLQ job by adding a new outbox event while preserving idempotency.
     *
     * @param jobId job id.
     * @param priority replay priority.
     * @return updated job response.
     */
    public JobResponse replayDlq(final UUID jobId, final JobPriority priority) {
        final Instant now = Instant.now(clock);
        return transactionTemplate.execute(status -> {
            final JobRecord job = requireJob(jobId);
            if (!JobStatus.DLQ.name().equals(job.status())) {
                throw new InvalidJobStateException("Only DLQ jobs can be replayed");
            }
            if (!jobStore.requeueDlqJob(job.id(), priority.name(), now)) {
                throw new InvalidJobStateException("Job left DLQ state concurrently");
            }
            final JobQueueMessage message = new JobQueueMessage(
                    job.id(), job.idempotencyKey(), job.attempt() + 1, now, job.traceId());
            jobStore.insertOutboxEvent(outbox(
                    job.id(),
                    job.idempotencyKey(),
                    topicForPriority(priority),
                    jsonSupport.write(message),
                    now));
            return JobResponse.from(requireJob(job.id()));
        });
    }

    /**
     * Cancels a job that is QUEUED, RETRY_SCHEDULED or in DLQ. A cancelled job is final:
     * it never runs again and cannot be replayed. Cancelling an already cancelled job
     * returns it unchanged, so the call is safe to repeat.
     *
     * <p>The job and its unsent outbox rows change in one transaction. Messages already on
     * Kafka are skipped by workers, because a worker can only start an attempt on a job that
     * is still waiting for it (see {@link JobStore#markProcessing}).</p>
     *
     * @param jobId job id.
     * @return cancelled job.
     * @throws InvalidJobStateException when the job is PROCESSING or SUCCEEDED.
     */
    public JobResponse cancel(final UUID jobId) {
        final Instant now = Instant.now(clock);
        return transactionTemplate.execute(status -> {
            if (jobStore.cancelJob(jobId, now)) {
                jobStore.cancelUnsentOutbox(jobId, now);
                meterRegistry.counter("jobqueue.jobs.cancelled").increment();
                return JobResponse.from(requireJob(jobId));
            }
            final JobRecord job = requireJob(jobId);
            if (JobStatus.CANCELLED.name().equals(job.status())) {
                return JobResponse.from(job);
            }
            throw new InvalidJobStateException("A " + job.status()
                    + " job cannot be cancelled; only QUEUED, RETRY_SCHEDULED or DLQ jobs can");
        });
    }

    /**
     * Returns current job counters.
     *
     * @return counts by status and by priority, plus mean successful attempt duration.
     */
    public JobStatsResponse stats() {
        final OptionalDouble average = jobStore.averageSucceededAttemptMillis();
        return new JobStatsResponse(
                jobStore.countJobsByStatus(),
                jobStore.countJobsByPriorityAndStatus(),
                average.isPresent() ? average.getAsDouble() : null);
    }

    private JobRecord requireJob(final UUID jobId) {
        return jobStore.findJobById(jobId).orElseThrow(() -> new JobNotFoundException(jobId));
    }

    private void ensureSameRequest(final JobRecord existing, final String requestHash) {
        if (!existing.requestHash().equals(requestHash)) {
            throw new IdempotencyConflictException(existing.idempotencyKey());
        }
    }

    private NewOutboxEvent outbox(
            final UUID jobId,
            final String idempotencyKey,
            final String topic,
            final String payloadJson,
            final Instant dueAt) {
        return new NewOutboxEvent(
                UUID.randomUUID(),
                jobId,
                topic,
                idempotencyKey,
                payloadJson,
                "{}",
                dueAt,
                Instant.now(clock));
    }

    private String topicForPriority(final JobPriority priority) {
        return priority == JobPriority.HIGH ? properties.getTopics().getHigh() : properties.getTopics().getLow();
    }

    /**
     * Hashes the request semantics. A record serializes its components in declaration
     * order, so every JVM produces the same hash for the same request; the previous
     * {@code Map.of} input had per-JVM randomized iteration order, which made a
     * replayed request look like a conflict when it reached a different instance.
     */
    private String requestHash(
            final String idempotencyKey,
            final String type,
            final JobPriority priority,
            final String payloadJson) {
        return hashSupport.sha256(jsonSupport.write(
                new RequestFingerprint(idempotencyKey, type, priority.name(), payloadJson)));
    }

    private static String normalize(final String value) {
        return value == null ? "" : value.trim();
    }

    private static String normalizeType(final String value) {
        return normalize(value).toUpperCase(Locale.ROOT);
    }

    private record RequestFingerprint(String idempotencyKey, String type, String priority, String payload) {
    }
}
