package com.github.pants.jobqueue.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.github.pants.jobqueue.domain.JobExecutionResult;
import com.github.pants.jobqueue.messaging.JobQueueMessage;
import com.github.pants.jobqueue.persistence.JobRecord;
import com.github.pants.jobqueue.persistence.JobStore;
import com.github.pants.jobqueue.support.IntegrationTestSupport;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.TaskScheduler;

/**
 * Concurrency guarantees of the worker path: duplicate deliveries, distributed locking,
 * receipts, outbox claiming, and where the work actually runs.
 */
class WorkerConcurrencyIntegrationTest extends IntegrationTestSupport {

    @Autowired
    private JobWorkerService workerService;

    @Autowired
    private DistributedLockService lockService;

    @Autowired
    private JobStore jobStore;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private TaskScheduler taskScheduler;

    @Autowired
    private ExpiredLeaseRecovery leaseRecovery;

    /**
     * The same message delivered many times at once (redelivery storm, consumer
     * rebalance) produces exactly one business side effect and ends SUCCEEDED.
     */
    @Test
    void duplicateDeliveriesProduceOneSideEffect() throws Exception {
        final UUID jobId = createJob(key("dup"), "HIGH", "{}", 4);
        final JobRecord job = jobStore.findJobById(jobId).orElseThrow();
        final String message = objectMapper.writeValueAsString(
                new JobQueueMessage(jobId, job.idempotencyKey(), 1, Instant.now(), job.traceId()));

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            final List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < 16; i++) {
                futures.add(executor.submit(() -> workerService.process(message, "jobs-high")));
            }
            for (final Future<?> future : futures) {
                future.get();
            }
        }

        await().atMost(Duration.ofSeconds(60)).until(() -> "SUCCEEDED".equals(statusOf(jobId)));
        assertThat(jdbcClient.sql("SELECT COUNT(*) FROM side_effect_receipts WHERE job_id = :id")
                .param("id", jobId).query(Long.class).single()).isEqualTo(1);
    }

    /**
     * Even with the Redis lock bypassed (expired lease, Redis failover), the unique
     * receipt key is the final guard: a second execution reports no new side effect.
     */
    @Test
    void receiptGuardsAgainstExecutionWithoutLock() {
        final UUID jobId = createJob(key("receipt"), "LOW", "{}", 4);
        final JobRecord job = jobStore.findJobById(jobId).orElseThrow();
        await().atMost(Duration.ofSeconds(60)).until(() -> "SUCCEEDED".equals(statusOf(jobId)));

        final JobExecutionResult again = jobExecutor.execute(job);

        assertThat(again.sideEffectCreated()).isFalse();
    }

    /**
     * While one owner holds a lock, no other caller enters the protected section.
     */
    @Test
    void lockGivesMutualExclusion() throws Exception {
        final String lockKey = "jobqueue:test-lock:" + UUID.randomUUID();
        final AtomicInteger inside = new AtomicInteger();
        final AtomicInteger maxInside = new AtomicInteger();
        final AtomicInteger ran = new AtomicInteger();
        final CountDownLatch start = new CountDownLatch(1);

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 20; i++) {
                executor.submit(() -> {
                    start.await();
                    return lockService.withLock(lockKey, Duration.ofSeconds(10), () -> {
                        maxInside.accumulateAndGet(inside.incrementAndGet(), Math::max);
                        ran.incrementAndGet();
                        sleep(300);
                        inside.decrementAndGet();
                    });
                });
            }
            start.countDown();
        }

        assertThat(maxInside.get()).isEqualTo(1);
        assertThat(ran.get()).isEqualTo(1);
        assertThat(redisTemplate.hasKey(lockKey)).isFalse();
    }

    /**
     * An owner whose lock expired must not delete the lock a new owner acquired since.
     */
    @Test
    void expiredOwnerDoesNotReleaseNewOwnersLock() {
        final String lockKey = "jobqueue:test-lock:" + UUID.randomUUID();

        lockService.withLock(lockKey, Duration.ofMillis(200), () -> {
            sleep(500);
            assertThat(redisTemplate.opsForValue().setIfAbsent(lockKey, "new-owner", Duration.ofSeconds(10))).isTrue();
        });

        assertThat(redisTemplate.opsForValue().get(lockKey)).isEqualTo("new-owner");
    }

    /**
     * A failing protected section still releases its lock.
     */
    @Test
    void lockIsReleasedWhenWorkFails() {
        final String lockKey = "jobqueue:test-lock:" + UUID.randomUUID();

        try {
            lockService.withLock(lockKey, Duration.ofSeconds(10), () -> {
                throw new IllegalStateException("boom");
            });
        } catch (IllegalStateException expected) {
            // expected
        }

        assertThat(redisTemplate.hasKey(lockKey)).isFalse();
    }

    /**
     * Concurrent publishers claim disjoint outbox rows ({@code FOR UPDATE SKIP LOCKED}).
     * Rows are due one hour ahead so the running publisher never sees them.
     */
    @Test
    void concurrentOutboxClaimsAreDisjoint() throws Exception {
        final Instant future = Instant.now().plus(Duration.ofHours(1));
        final UUID jobId = insertJobWithoutOutbox("claim");
        for (int i = 0; i < 40; i++) {
            insertOutbox(jobId, "PENDING", future, future);
        }

        final Set<UUID> claimed = ConcurrentHashMap.newKeySet();
        final AtomicInteger total = new AtomicInteger();
        final Instant claimTime = future.plusSeconds(1);
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 8; i++) {
                // Each publisher drains until nothing is left; any row claimed twice would show up below.
                executor.submit(() -> {
                    List<com.github.pants.jobqueue.persistence.OutboxRecord> batch;
                    do {
                        batch = jobStore.claimDueOutbox(claimTime, Instant.EPOCH, 5);
                        batch.stream().filter(row -> row.jobId().equals(jobId)).forEach(row -> {
                            claimed.add(row.id());
                            total.incrementAndGet();
                        });
                    } while (!batch.isEmpty());
                });
            }
        }

        assertThat(total.get()).isEqualTo(40);
        assertThat(claimed).hasSize(40);
    }

    /**
     * A row left in PUBLISHING by a crashed publisher becomes claimable after the
     * publishing timeout; a fresh PUBLISHING row does not.
     */
    @Test
    void stuckPublishingRowIsReclaimed() {
        final Instant future = Instant.now().plus(Duration.ofHours(2));
        final UUID jobId = insertJobWithoutOutbox("stuck");
        final UUID stuck = insertOutbox(jobId, "PUBLISHING", future, future);
        final UUID fresh = insertOutbox(jobId, "PUBLISHING", future, future.plus(Duration.ofMinutes(30)));

        final List<UUID> claimed = jobStore.claimDueOutbox(future, future.plus(Duration.ofMinutes(5)), 100)
                .stream().map(row -> row.id()).toList();

        assertThat(claimed).contains(stuck).doesNotContain(fresh);
    }

    /**
     * A job stranded in PROCESSING by a dead worker is retried once its lease expires, and
     * the dead worker, should it come back, can no longer overwrite the job.
     */
    @Test
    void strandedJobIsRecoveredAndRetried() {
        final UUID jobId = insertStrandedJob("stranded", 4, Instant.now().minusSeconds(1));

        leaseRecovery.recoverExpiredLeases();

        final JobRecord job = jobStore.findJobById(jobId).orElseThrow();
        assertThat(job.status()).isEqualTo("RETRY_SCHEDULED");
        assertThat(job.errorMessage()).isEqualTo("Worker lease expired before the attempt finished");
        assertThat(jdbcClient.sql("SELECT status FROM job_attempts WHERE job_id = :id AND attempt = 1")
                .param("id", jobId).query(String.class).single()).isEqualTo("RETRY_SCHEDULED");
        assertThat(jobStore.markSucceeded(jobId, 1, "{}", Instant.now())).isFalse();

        await().atMost(Duration.ofSeconds(60)).until(() -> "SUCCEEDED".equals(statusOf(jobId)));
        assertThat(jobStore.findJobById(jobId).orElseThrow().attempt()).isEqualTo(2);
    }

    /**
     * A stranded job on its last attempt goes to DLQ like any other final failure.
     */
    @Test
    void strandedJobOnLastAttemptGoesToDlq() {
        final UUID jobId = insertStrandedJob("stranded-last", 1, Instant.now().minusSeconds(1));

        leaseRecovery.recoverExpiredLeases();

        assertThat(statusOf(jobId)).isEqualTo("DLQ");
    }

    /**
     * A job whose lease is still running belongs to its worker and is left alone.
     */
    @Test
    void jobWithLiveLeaseIsNotRecovered() {
        final UUID jobId = insertStrandedJob("live-lease", 4, Instant.now().plus(Duration.ofMinutes(5)));

        leaseRecovery.recoverExpiredLeases();

        assertThat(statusOf(jobId)).isEqualTo("PROCESSING");
        assertThat(jobStore.markSucceeded(jobId, 1, "{}", Instant.now())).isTrue();
    }

    /**
     * HTTP requests, Kafka listeners, and scheduled tasks all run on virtual threads.
     */
    @Test
    void requestsListenersAndSchedulerRunOnVirtualThreads() throws Exception {
        final Set<Boolean> executorThreads = ConcurrentHashMap.newKeySet();
        final Set<Boolean> requestThreads = ConcurrentHashMap.newKeySet();
        Mockito.doAnswer(invocation -> {
            executorThreads.add(Thread.currentThread().isVirtual());
            return invocation.callRealMethod();
        }).when(jobExecutor).execute(Mockito.any());
        Mockito.doAnswer(invocation -> {
            requestThreads.add(Thread.currentThread().isVirtual());
            return invocation.callRealMethod();
        }).when(jobCommandService).create(Mockito.any());

        try {
            final UUID jobId = createJob(key("virtual"), "HIGH", "{}", 4);
            await().atMost(Duration.ofSeconds(60)).until(() -> "SUCCEEDED".equals(statusOf(jobId)));

            final CountDownLatch ran = new CountDownLatch(1);
            final Set<Boolean> schedulerThreads = ConcurrentHashMap.newKeySet();
            taskScheduler.schedule(() -> {
                schedulerThreads.add(Thread.currentThread().isVirtual());
                ran.countDown();
            }, Instant.now());
            assertThat(ran.await(10, TimeUnit.SECONDS)).isTrue();

            assertThat(requestThreads).containsExactly(true);
            assertThat(executorThreads).containsExactly(true);
            assertThat(schedulerThreads).containsExactly(true);
        } finally {
            Mockito.reset(jobExecutor, jobCommandService);
        }
    }

    /** A job left PROCESSING at attempt 1 by a worker that never finished, with no message left for it. */
    private UUID insertStrandedJob(final String name, final int maxAttempts, final Instant leasedUntil) {
        final UUID jobId = UUID.randomUUID();
        final Instant startedAt = Instant.now().minus(Duration.ofMinutes(10));
        jobStore.insertJobIfAbsent(new com.github.pants.jobqueue.persistence.NewJob(
                jobId, key(name), "hash", "TEST", "LOW", "{}", maxAttempts, "trace", startedAt));
        jobStore.markProcessing(jobId, 1, "dead-worker", startedAt, leasedUntil);
        jobStore.insertAttemptStarted(jobId, 1, "jobs-low", startedAt);
        return jobId;
    }

    private UUID insertJobWithoutOutbox(final String name) {
        final UUID jobId = UUID.randomUUID();
        jobStore.insertJobIfAbsent(new com.github.pants.jobqueue.persistence.NewJob(
                jobId, key(name), "hash", "TEST", "LOW", "{}", 1, "trace", Instant.now()));
        return jobId;
    }

    private UUID insertOutbox(final UUID jobId, final String status, final Instant notBefore, final Instant updatedAt) {
        final UUID id = UUID.randomUUID();
        jdbcClient.sql("""
                        INSERT INTO job_outbox (id, job_id, topic, message_key, payload_json, headers_json, status,
                                                attempts, not_before, created_at, updated_at)
                        VALUES (:id, :jobId, 'jobs-low', 'k', '{}', '{}', :status, 0, :notBefore, now(), :updatedAt)
                        """)
                .param("id", id)
                .param("jobId", jobId)
                .param("status", status)
                .param("notBefore", notBefore.atOffset(java.time.ZoneOffset.UTC))
                .param("updatedAt", updatedAt.atOffset(java.time.ZoneOffset.UTC))
                .update();
        return id;
    }

    private static void sleep(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(exception);
        }
    }
}
