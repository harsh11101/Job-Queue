package com.github.pants.jobqueue.service;

import com.github.pants.jobqueue.persistence.JobRecord;
import com.github.pants.jobqueue.persistence.JobStore;
import java.time.Clock;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Periodically moves on jobs stranded in {@code PROCESSING} by a worker that died.
 *
 * <p>When a worker dies mid-attempt, Kafka redelivers its message within seconds, but the
 * dead worker's Redis lease is still held, so the new consumer skips and acknowledges it.
 * After that no message exists for the job. Once the lease has expired (the Redis lock
 * expires no later than {@code jobs.leased_until}), this task ends the attempt as a
 * retryable failure through the normal retry path.</p>
 *
 * <p>Every replica runs it. That is safe without coordination because each recovery is a
 * fenced update: only one party can end a given attempt.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ExpiredLeaseRecovery {

    /** Jobs recovered per run; any remainder is picked up by the next run. */
    static final int BATCH_SIZE = 100;

    private final Clock clock;
    private final JobStore jobStore;
    private final JobWorkerService workerService;

    /**
     * Recovers every job whose lease expired, up to {@link #BATCH_SIZE} per run.
     */
    @Scheduled(fixedDelayString = "${app.job-queue.worker.lease-recovery-delay-ms:30000}")
    public void recoverExpiredLeases() {
        try {
            for (final JobRecord job : jobStore.findExpiredLeases(Instant.now(clock), BATCH_SIZE)) {
                try {
                    workerService.recoverExpiredLease(job);
                } catch (RuntimeException exception) {
                    log.error("Could not recover job {} with an expired lease", job.id(), exception);
                }
            }
        } catch (RuntimeException exception) {
            log.error("Expired lease recovery run failed", exception);
        }
    }
}
