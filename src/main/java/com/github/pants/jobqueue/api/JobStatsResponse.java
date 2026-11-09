package com.github.pants.jobqueue.api;

import java.util.Map;

/**
 * Aggregated job counters.
 *
 * @param countsByStatus map where key is a durable job status and value is the number of jobs.
 * @param countsByPriority map of priority to (status to number of jobs).
 * @param averageSucceededAttemptMillis mean duration of successful attempts, null before the first success.
 */
public record JobStatsResponse(
        Map<String, Long> countsByStatus,
        Map<String, Map<String, Long>> countsByPriority,
        Double averageSucceededAttemptMillis) {
}
