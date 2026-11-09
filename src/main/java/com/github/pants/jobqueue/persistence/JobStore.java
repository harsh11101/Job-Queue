package com.github.pants.jobqueue.persistence;

import com.github.pants.jobqueue.domain.JobStatus;
import com.github.pants.jobqueue.domain.OutboxStatus;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.TreeMap;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Blocking PostgreSQL data access layer for jobs, outbox events, attempts, and
 * side-effect receipts.
 *
 * <p>The store intentionally uses SQL for critical state transitions because job
 * queue correctness depends on explicit, reviewable update semantics. Every method
 * joins the caller's Spring-managed transaction when one is active.</p>
 *
 * <p>The transitions that end an attempt (succeeded, retry scheduled, DLQ) are fenced:
 * they apply only while the job is still {@code PROCESSING} at that attempt number.
 * Once an expired lease has been recovered, the original worker, if it is still
 * running, can no longer overwrite the job; exactly one party ends each attempt.</p>
 */
@Repository
@RequiredArgsConstructor
public class JobStore {

    private final JdbcClient jdbcClient;

    /**
     * Inserts a job if its idempotency key was not used before.
     *
     * @param job normalized new job command.
     * @return true when inserted, false when the key already exists.
     */
    public boolean insertJobIfAbsent(final NewJob job) {
        return jdbcClient.sql("""
                        INSERT INTO jobs (
                            id, idempotency_key, request_hash, type, priority, status,
                            payload_json, trace_id, attempt, max_attempts, created_at, updated_at
                        ) VALUES (
                            :id, :idempotencyKey, :requestHash, :type, :priority, :status,
                            :payloadJson, :traceId, 0, :maxAttempts, :createdAt, :updatedAt
                        )
                        ON CONFLICT (idempotency_key) DO NOTHING
                        """)
                .param("id", job.id())
                .param("idempotencyKey", job.idempotencyKey())
                .param("requestHash", job.requestHash())
                .param("type", job.type())
                .param("priority", job.priority())
                .param("status", JobStatus.QUEUED.name())
                .param("payloadJson", job.payloadJson())
                .param("traceId", job.traceId())
                .param("maxAttempts", job.maxAttempts())
                .param("createdAt", toOffset(job.now()))
                .param("updatedAt", toOffset(job.now()))
                .update() == 1;
    }

    /**
     * Finds a job by id.
     *
     * @param id job id.
     * @return job record when present.
     */
    public Optional<JobRecord> findJobById(final UUID id) {
        return jdbcClient.sql("SELECT * FROM jobs WHERE id = :id")
                .param("id", id)
                .query(this::mapJob)
                .optional();
    }

    /**
     * Finds a job by idempotency key.
     *
     * @param idempotencyKey idempotency key.
     * @return job record when present.
     */
    public Optional<JobRecord> findJobByIdempotencyKey(final String idempotencyKey) {
        return jdbcClient.sql("SELECT * FROM jobs WHERE idempotency_key = :idempotencyKey")
                .param("idempotencyKey", idempotencyKey)
                .query(this::mapJob)
                .optional();
    }

    /**
     * Returns one page of jobs matching the filter, newest first.
     *
     * @param filter search filter; null fields are ignored.
     * @param page zero-based page index.
     * @param size page size.
     * @return page content and total match count.
     */
    public JobPage searchJobs(final JobSearchFilter filter, final int page, final int size) {
        final List<String> conditions = new ArrayList<>();
        final Map<String, Object> params = new LinkedHashMap<>();
        if (filter.status() != null) {
            conditions.add("status = :status");
            params.put("status", filter.status());
        }
        if (filter.priority() != null) {
            conditions.add("priority = :priority");
            params.put("priority", filter.priority());
        }
        if (filter.type() != null) {
            conditions.add("type = :type");
            params.put("type", filter.type());
        }
        if (filter.createdFrom() != null) {
            conditions.add("created_at >= :createdFrom");
            params.put("createdFrom", toOffset(filter.createdFrom()));
        }
        if (filter.createdTo() != null) {
            conditions.add("created_at < :createdTo");
            params.put("createdTo", toOffset(filter.createdTo()));
        }
        final String where = conditions.isEmpty() ? "" : " WHERE " + String.join(" AND ", conditions);

        final long total = jdbcClient.sql("SELECT COUNT(*) FROM jobs" + where)
                .params(params)
                .query(Long.class)
                .single();
        final List<JobRecord> items = jdbcClient.sql(
                        "SELECT * FROM jobs" + where + " ORDER BY created_at DESC, id LIMIT :limit OFFSET :offset")
                .params(params)
                .param("limit", size)
                .param("offset", (long) page * size)
                .query(this::mapJob)
                .list();
        return new JobPage(items, total);
    }

    /**
     * Lists the audit rows of every attempt of a job, oldest first.
     *
     * @param jobId job id.
     * @return attempt rows.
     */
    public List<JobAttemptRecord> findAttempts(final UUID jobId) {
        return jdbcClient.sql("SELECT * FROM job_attempts WHERE job_id = :jobId ORDER BY attempt")
                .param("jobId", jobId)
                .query((rs, rowNum) -> new JobAttemptRecord(
                        rs.getInt("attempt"),
                        rs.getString("status"),
                        rs.getString("topic"),
                        rs.getString("error_message"),
                        rs.getString("failure_class"),
                        instant(rs, "started_at"),
                        instant(rs, "finished_at")))
                .list();
    }

    /**
     * Inserts a transactional outbox event.
     *
     * @param event outbox event command.
     */
    public void insertOutboxEvent(final NewOutboxEvent event) {
        jdbcClient.sql("""
                        INSERT INTO job_outbox (
                            id, job_id, topic, message_key, payload_json, headers_json,
                            status, attempts, not_before, created_at, updated_at
                        ) VALUES (
                            :id, :jobId, :topic, :messageKey, :payloadJson, :headersJson,
                            :status, 0, :notBefore, :createdAt, :updatedAt
                        )
                        """)
                .param("id", event.id())
                .param("jobId", event.jobId())
                .param("topic", event.topic())
                .param("messageKey", event.messageKey())
                .param("payloadJson", event.payloadJson())
                .param("headersJson", event.headersJson())
                .param("status", OutboxStatus.PENDING.name())
                .param("notBefore", toOffset(event.notBefore()))
                .param("createdAt", toOffset(event.now()))
                .param("updatedAt", toOffset(event.now()))
                .update();
    }

    /**
     * Claims due outbox rows for publication.
     *
     * <p>Due {@code PENDING}/{@code FAILED} rows are claimed, and so are rows stuck in
     * {@code PUBLISHING} since before {@code staleBefore}: their publisher died between
     * claim and completion. Reclaiming may publish a message twice, which consumers
     * tolerate because delivery is already at-least-once.</p>
     *
     * @param now current timestamp.
     * @param staleBefore rows still {@code PUBLISHING} with an older update are reclaimed.
     * @param limit batch limit.
     * @return claimed rows.
     */
    public List<OutboxRecord> claimDueOutbox(final Instant now, final Instant staleBefore, final int limit) {
        return jdbcClient.sql("""
                        WITH claimed AS (
                            SELECT id
                            FROM job_outbox
                            WHERE (status IN ('PENDING', 'FAILED') AND not_before <= :now)
                               OR (status = 'PUBLISHING' AND updated_at <= :staleBefore)
                            ORDER BY created_at
                            LIMIT :limit
                            FOR UPDATE SKIP LOCKED
                        )
                        UPDATE job_outbox outbox
                        SET status = 'PUBLISHING', attempts = attempts + 1, updated_at = :now
                        FROM claimed
                        WHERE outbox.id = claimed.id
                        RETURNING outbox.*
                        """)
                .param("now", toOffset(now))
                .param("staleBefore", toOffset(staleBefore))
                .param("limit", limit)
                .query((rs, rowNum) -> new OutboxRecord(
                        rs.getObject("id", UUID.class),
                        rs.getObject("job_id", UUID.class),
                        rs.getString("topic"),
                        rs.getString("message_key"),
                        rs.getString("payload_json"),
                        rs.getString("headers_json"),
                        rs.getInt("attempts"),
                        instant(rs, "not_before"),
                        instant(rs, "created_at")))
                .list();
    }

    /**
     * Marks an outbox row as published.
     *
     * @param id outbox row id.
     * @param now timestamp.
     */
    public void markOutboxPublished(final UUID id, final Instant now) {
        jdbcClient.sql("""
                        UPDATE job_outbox
                        SET status = 'PUBLISHED', published_at = :now, updated_at = :now, last_error = NULL
                        WHERE id = :id
                        """)
                .param("id", id)
                .param("now", toOffset(now))
                .update();
    }

    /**
     * Marks an outbox row as failed so the scheduler can retry publication.
     *
     * @param id outbox row id.
     * @param error safe error message.
     * @param now timestamp.
     */
    public void markOutboxFailed(final UUID id, final String error, final Instant now) {
        jdbcClient.sql("""
                        UPDATE job_outbox
                        SET status = 'FAILED', last_error = :error, updated_at = :now
                        WHERE id = :id
                        """)
                .param("id", id)
                .param("error", error)
                .param("now", toOffset(now))
                .update();
    }

    /**
     * Finds jobs still {@code PROCESSING} after their worker lease ended. Their worker
     * most likely died mid-attempt, and nothing else would ever move them on.
     *
     * @param now current time.
     * @param limit maximum rows returned.
     * @return expired jobs, longest expired first.
     */
    public List<JobRecord> findExpiredLeases(final Instant now, final int limit) {
        return jdbcClient.sql("""
                        SELECT * FROM jobs
                        WHERE status = 'PROCESSING' AND leased_until < :now
                        ORDER BY leased_until
                        LIMIT :limit
                        """)
                .param("now", toOffset(now))
                .param("limit", limit)
                .query(this::mapJob)
                .list();
    }

    /**
     * Starts an attempt: marks the job as processing and records worker lease metadata,
     * if the job may still run this attempt.
     *
     * <p>This is the fence between running and cancelling. It matches a job waiting for
     * this attempt ({@code QUEUED} or {@code RETRY_SCHEDULED} at a lower attempt number)
     * or one already {@code PROCESSING} this same attempt (a redelivery after a transient
     * error; the caller holds the lease). A cancel commits through the same row, so either
     * the cancel or the start wins, never both. Stale messages for an attempt that has
     * already been ended do not match either.</p>
     *
     * @param jobId job id.
     * @param attempt attempt number.
     * @param workerId worker instance id.
     * @param now timestamp.
     * @param leasedUntil lease expiry timestamp.
     * @return true when the attempt started, false when the job may not run it.
     */
    public boolean markProcessing(
            final UUID jobId,
            final int attempt,
            final String workerId,
            final Instant now,
            final Instant leasedUntil) {
        return jdbcClient.sql("""
                        UPDATE jobs
                        SET status = :status,
                            attempt = :attempt,
                            locked_at = :now,
                            leased_until = :leasedUntil,
                            worker_id = :workerId,
                            updated_at = :now,
                            version = version + 1
                        WHERE id = :jobId
                          AND ((status IN ('QUEUED', 'RETRY_SCHEDULED') AND attempt < :attempt)
                               OR (status = 'PROCESSING' AND attempt = :attempt))
                        """)
                .param("jobId", jobId)
                .param("status", JobStatus.PROCESSING.name())
                .param("attempt", attempt)
                .param("workerId", workerId)
                .param("now", toOffset(now))
                .param("leasedUntil", toOffset(leasedUntil))
                .update() > 0;
    }

    /**
     * Cancels a job that is waiting to run or sits in DLQ.
     *
     * <p>Fenced on the status: a job that a worker has already started (PROCESSING), or
     * that has finished, does not match. Concurrent cancels are serialized by the row lock
     * and only the first one matches.</p>
     *
     * @param jobId job id.
     * @param now timestamp.
     * @return true when this call cancelled the job.
     */
    public boolean cancelJob(final UUID jobId, final Instant now) {
        return jdbcClient.sql("""
                        UPDATE jobs
                        SET status = :status,
                            next_retry_at = NULL,
                            leased_until = NULL,
                            worker_id = NULL,
                            completed_at = :now,
                            updated_at = :now,
                            version = version + 1
                        WHERE id = :jobId AND status IN ('QUEUED', 'RETRY_SCHEDULED', 'DLQ')
                        """)
                .param("jobId", jobId)
                .param("status", JobStatus.CANCELLED.name())
                .param("now", toOffset(now))
                .update() > 0;
    }

    /**
     * Withdraws a cancelled job's outbox rows that have not been sent yet, so they are never
     * published. Rows already being sent cannot be recalled; workers skip their messages.
     *
     * @param jobId job id.
     * @param now timestamp.
     * @return number of rows withdrawn.
     */
    public int cancelUnsentOutbox(final UUID jobId, final Instant now) {
        return jdbcClient.sql("""
                        UPDATE job_outbox
                        SET status = :status, updated_at = :now
                        WHERE job_id = :jobId AND status IN ('PENDING', 'FAILED')
                        """)
                .param("jobId", jobId)
                .param("status", OutboxStatus.CANCELLED.name())
                .param("now", toOffset(now))
                .update();
    }

    /**
     * Records an attempt start in the audit table.
     *
     * @param jobId job id.
     * @param attempt attempt number.
     * @param topic Kafka topic consumed.
     * @param now timestamp.
     */
    public void insertAttemptStarted(final UUID jobId, final int attempt, final String topic, final Instant now) {
        jdbcClient.sql("""
                        INSERT INTO job_attempts (id, job_id, attempt, status, topic, started_at)
                        VALUES (:id, :jobId, :attempt, :status, :topic, :startedAt)
                        ON CONFLICT (job_id, attempt) DO NOTHING
                        """)
                .param("id", UUID.randomUUID())
                .param("jobId", jobId)
                .param("attempt", attempt)
                .param("status", JobStatus.PROCESSING.name())
                .param("topic", topic)
                .param("startedAt", toOffset(now))
                .update();
    }

    /**
     * Finishes an attempt audit row.
     *
     * @param jobId job id.
     * @param attempt attempt number.
     * @param status final attempt status.
     * @param error error message, may be null.
     * @param failureClass exception class name, may be null.
     * @param now timestamp.
     */
    public void finishAttempt(
            final UUID jobId,
            final int attempt,
            final String status,
            final String error,
            final String failureClass,
            final Instant now) {
        jdbcClient.sql("""
                        UPDATE job_attempts
                        SET status = :status,
                            error_message = :error,
                            failure_class = :failureClass,
                            finished_at = :finishedAt
                        WHERE job_id = :jobId AND attempt = :attempt
                        """)
                .param("jobId", jobId)
                .param("attempt", attempt)
                .param("status", status)
                .param("error", error, Types.VARCHAR)
                .param("failureClass", failureClass, Types.VARCHAR)
                .param("finishedAt", toOffset(now))
                .update();
    }

    /**
     * Inserts a side-effect receipt using a unique effect key.
     *
     * @param effectKey business effect key.
     * @param jobId job id.
     * @param jobType job type.
     * @param payloadHash SHA-256 payload hash.
     * @param now timestamp.
     * @return true when this call created the receipt, false when it already existed.
     */
    public boolean insertSideEffectReceipt(
            final String effectKey,
            final UUID jobId,
            final String jobType,
            final String payloadHash,
            final Instant now) {
        return jdbcClient.sql("""
                        INSERT INTO side_effect_receipts (id, effect_key, job_id, job_type, payload_hash, created_at)
                        VALUES (:id, :effectKey, :jobId, :jobType, :payloadHash, :createdAt)
                        ON CONFLICT (effect_key) DO NOTHING
                        """)
                .param("id", UUID.randomUUID())
                .param("effectKey", effectKey)
                .param("jobId", jobId)
                .param("jobType", jobType)
                .param("payloadHash", payloadHash)
                .param("createdAt", toOffset(now))
                .update() == 1;
    }

    /**
     * Marks a job as successfully completed, if the finishing attempt still owns it.
     *
     * @param jobId job id.
     * @param attempt attempt that finished; the update is fenced on it (see class notes).
     * @param resultJson result JSON.
     * @param now timestamp.
     * @return true when the transition happened, false when the attempt no longer owns the job.
     */
    public boolean markSucceeded(final UUID jobId, final int attempt, final String resultJson, final Instant now) {
        return jdbcClient.sql("""
                        UPDATE jobs
                        SET status = :status,
                            result_json = :resultJson,
                            error_message = NULL,
                            failure_class = NULL,
                            next_retry_at = NULL,
                            leased_until = NULL,
                            worker_id = NULL,
                            completed_at = :now,
                            updated_at = :now,
                            version = version + 1
                        WHERE id = :jobId AND status = 'PROCESSING' AND attempt = :attempt
                        """)
                .param("jobId", jobId)
                .param("attempt", attempt)
                .param("status", JobStatus.SUCCEEDED.name())
                .param("resultJson", resultJson)
                .param("now", toOffset(now))
                .update() > 0;
    }

    /**
     * Marks a job as retry scheduled, if the failed attempt still owns it.
     *
     * @param jobId job id.
     * @param attempt attempt that failed; the update is fenced on it (see class notes).
     * @param error error message.
     * @param failureClass exception class name.
     * @param nextRetryAt next retry due timestamp.
     * @param now timestamp.
     * @return true when the transition happened, false when the attempt no longer owns the job.
     */
    public boolean markRetryScheduled(
            final UUID jobId,
            final int attempt,
            final String error,
            final String failureClass,
            final Instant nextRetryAt,
            final Instant now) {
        return jdbcClient.sql("""
                        UPDATE jobs
                        SET status = :status,
                            error_message = :error,
                            failure_class = :failureClass,
                            next_retry_at = :nextRetryAt,
                            leased_until = NULL,
                            worker_id = NULL,
                            updated_at = :now,
                            version = version + 1
                        WHERE id = :jobId AND status = 'PROCESSING' AND attempt = :attempt
                        """)
                .param("jobId", jobId)
                .param("attempt", attempt)
                .param("status", JobStatus.RETRY_SCHEDULED.name())
                .param("error", error)
                .param("failureClass", failureClass)
                .param("nextRetryAt", toOffset(nextRetryAt))
                .param("now", toOffset(now))
                .update() > 0;
    }

    /**
     * Marks a job as sent to DLQ, if the failed attempt still owns it.
     *
     * @param jobId job id.
     * @param attempt attempt that failed; the update is fenced on it (see class notes).
     * @param error error message.
     * @param failureClass exception class name.
     * @param now timestamp.
     * @return true when the transition happened, false when the attempt no longer owns the job.
     */
    public boolean markDlq(
            final UUID jobId, final int attempt, final String error, final String failureClass, final Instant now) {
        return jdbcClient.sql("""
                        UPDATE jobs
                        SET status = :status,
                            error_message = :error,
                            failure_class = :failureClass,
                            next_retry_at = NULL,
                            leased_until = NULL,
                            worker_id = NULL,
                            completed_at = :now,
                            updated_at = :now,
                            version = version + 1
                        WHERE id = :jobId AND status = 'PROCESSING' AND attempt = :attempt
                        """)
                .param("jobId", jobId)
                .param("attempt", attempt)
                .param("status", JobStatus.DLQ.name())
                .param("error", error)
                .param("failureClass", failureClass)
                .param("now", toOffset(now))
                .update() > 0;
    }

    /**
     * Requeues a DLQ job for operator replay.
     *
     * <p>The {@code status = 'DLQ'} guard makes concurrent replays safe: the row lock
     * serializes them and only the first one matches.</p>
     *
     * @param jobId job id.
     * @param priority target priority.
     * @param now timestamp.
     * @return true when the job was requeued, false when it was no longer in DLQ.
     */
    public boolean requeueDlqJob(final UUID jobId, final String priority, final Instant now) {
        return jdbcClient.sql("""
                        UPDATE jobs
                        SET status = :status,
                            priority = :priority,
                            next_retry_at = NULL,
                            leased_until = NULL,
                            worker_id = NULL,
                            updated_at = :now,
                            version = version + 1
                        WHERE id = :jobId AND status = 'DLQ'
                        """)
                .param("jobId", jobId)
                .param("status", JobStatus.QUEUED.name())
                .param("priority", priority)
                .param("now", toOffset(now))
                .update() > 0;
    }

    /**
     * Counts jobs by status.
     *
     * @return map of status to count, ordered by status name.
     */
    public Map<String, Long> countJobsByStatus() {
        final Map<String, Long> counts = new TreeMap<>();
        jdbcClient.sql("SELECT status, COUNT(*) AS total FROM jobs GROUP BY status")
                .query(rs -> {
                    counts.put(rs.getString("status"), rs.getLong("total"));
                });
        return counts;
    }

    /**
     * Counts jobs by priority and status.
     *
     * @return map of priority to (status to count), both ordered by name.
     */
    public Map<String, Map<String, Long>> countJobsByPriorityAndStatus() {
        final Map<String, Map<String, Long>> counts = new TreeMap<>();
        jdbcClient.sql("SELECT priority, status, COUNT(*) AS total FROM jobs GROUP BY priority, status")
                .query(rs -> {
                    counts.computeIfAbsent(rs.getString("priority"), key -> new TreeMap<>())
                            .put(rs.getString("status"), rs.getLong("total"));
                });
        return counts;
    }

    /**
     * Calculates the mean wall-clock duration of successful attempts.
     *
     * @return mean duration in milliseconds, empty when no attempt has succeeded yet.
     */
    public OptionalDouble averageSucceededAttemptMillis() {
        final Double average = jdbcClient.sql("""
                        SELECT AVG(EXTRACT(EPOCH FROM (finished_at - started_at)) * 1000)
                        FROM job_attempts
                        WHERE status = 'SUCCEEDED' AND finished_at IS NOT NULL
                        """)
                .query(Double.class)
                .single();
        return average == null ? OptionalDouble.empty() : OptionalDouble.of(average);
    }

    private JobRecord mapJob(final ResultSet rs, final int rowNum) throws SQLException {
        return new JobRecord(
                rs.getObject("id", UUID.class),
                rs.getString("idempotency_key"),
                rs.getString("request_hash"),
                rs.getString("type"),
                rs.getString("priority"),
                rs.getString("status"),
                rs.getString("payload_json"),
                rs.getString("result_json"),
                rs.getString("error_message"),
                rs.getString("failure_class"),
                rs.getString("trace_id"),
                rs.getInt("attempt"),
                rs.getInt("max_attempts"),
                instant(rs, "next_retry_at"),
                instant(rs, "created_at"),
                instant(rs, "updated_at"),
                instant(rs, "completed_at"));
    }

    private static OffsetDateTime toOffset(final Instant instant) {
        return instant.atOffset(ZoneOffset.UTC);
    }

    private static Instant instant(final ResultSet rs, final String column) throws SQLException {
        final OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }
}
