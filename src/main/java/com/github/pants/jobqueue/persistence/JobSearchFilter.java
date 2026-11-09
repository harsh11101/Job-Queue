package com.github.pants.jobqueue.persistence;

import java.time.Instant;

/**
 * Optional criteria for listing jobs. A null field does not restrict the result.
 *
 * @param status durable job status.
 * @param priority priority lane.
 * @param type normalized (upper-case) job type.
 * @param createdFrom inclusive lower bound of the creation timestamp.
 * @param createdTo exclusive upper bound of the creation timestamp.
 */
public record JobSearchFilter(
        String status,
        String priority,
        String type,
        Instant createdFrom,
        Instant createdTo) {
}
