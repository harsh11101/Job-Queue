package com.github.pants.jobqueue.api;

import com.github.pants.jobqueue.domain.JobPriority;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import tools.jackson.databind.JsonNode;

/**
 * HTTP request used to create a job.
 *
 * @param idempotencyKey business idempotency key supplied by the caller.
 * @param type job type used by workers to select execution logic.
 * @param priority priority lane for initial routing.
 * @param payload arbitrary JSON payload.
 * @param maxAttempts maximum number of processing attempts before DLQ.
 */
public record CreateJobRequest(
        @NotBlank @Size(max = 200) String idempotencyKey,
        @NotBlank @Size(max = 100) String type,
        @NotNull JobPriority priority,
        @NotNull JsonNode payload,
        @Min(1) @Max(20) Integer maxAttempts) {
}
