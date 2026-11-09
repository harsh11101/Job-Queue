package com.github.pants.jobqueue.api;

import com.github.pants.jobqueue.domain.JobPriority;
import jakarta.validation.constraints.NotNull;

/**
 * HTTP request used by operators to replay a DLQ job safely.
 *
 * @param priority priority lane used for the replayed message.
 */
public record ReplayJobRequest(@NotNull JobPriority priority) {
}
