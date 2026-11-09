package com.github.pants.jobqueue.service;

import com.github.pants.jobqueue.domain.JobStatus;
import com.github.pants.jobqueue.persistence.JobStore;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Publishes how much work is in the queue right now, as gauges:
 * {@code jobqueue.jobs.current{status}} and {@code jobqueue.outbox.unsent}.
 *
 * <p>The counters elsewhere record events; these answer "how many jobs are waiting,
 * retrying or dead-lettered at this moment". The values are read from PostgreSQL on a
 * schedule rather than on every scrape, so scraping never queries the database. Every
 * replica reports the same database-wide numbers, so dashboards take the max.</p>
 */
@Slf4j
@Component
public class QueueDepthMetrics {

    private final JobStore jobStore;
    private final Map<String, AtomicLong> jobsByStatus = new LinkedHashMap<>();
    private final AtomicLong unsentOutbox = new AtomicLong();

    /**
     * Registers one gauge per job status and one for the outbox backlog.
     *
     * @param jobStore job store queried for the counts.
     * @param meterRegistry registry the gauges are published to.
     */
    public QueueDepthMetrics(final JobStore jobStore, final MeterRegistry meterRegistry) {
        this.jobStore = jobStore;
        for (final JobStatus status : JobStatus.values()) {
            if (status == JobStatus.ACCEPTED) {
                continue; // declared in the enum but never written
            }
            final AtomicLong value = new AtomicLong();
            jobsByStatus.put(status.name(), value);
            Gauge.builder("jobqueue.jobs.current", value, AtomicLong::get)
                    .description("Jobs currently in each status")
                    .tag("status", status.name())
                    .register(meterRegistry);
        }
        Gauge.builder("jobqueue.outbox.unsent", unsentOutbox, AtomicLong::get)
                .description("Outbox rows not yet confirmed by Kafka")
                .register(meterRegistry);
    }

    /**
     * Refreshes the gauges from the database every 15 seconds, starting at startup.
     */
    @Scheduled(fixedDelay = 15_000)
    public void refresh() {
        try {
            final Map<String, Long> counts = jobStore.countJobsByStatus();
            jobsByStatus.forEach((status, value) -> value.set(counts.getOrDefault(status, 0L)));
            unsentOutbox.set(jobStore.countUnsentOutbox());
        } catch (RuntimeException exception) {
            // Keep the last values; a failed refresh must not affect job processing.
            log.warn("Could not refresh queue depth metrics", exception);
        }
    }
}
