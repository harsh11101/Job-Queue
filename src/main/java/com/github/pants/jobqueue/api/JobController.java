package com.github.pants.jobqueue.api;

import com.github.pants.jobqueue.domain.JobPriority;
import com.github.pants.jobqueue.domain.JobStatus;
import com.github.pants.jobqueue.service.JobCommandService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * HTTP API for creating, reading, listing, replaying, and monitoring jobs.
 *
 * <p>Handlers are plain blocking methods; with {@code spring.threads.virtual.enabled}
 * Tomcat runs each request on its own virtual thread.</p>
 */
@RestController
@RequestMapping("/api/v1/jobs")
@RequiredArgsConstructor
public class JobController {

    private final JobCommandService jobCommandService;

    /**
     * Creates a job or returns the existing idempotent job.
     *
     * @param request create request.
     * @return 201 for a new job, 200 for an idempotent duplicate.
     */
    @PostMapping
    public ResponseEntity<JobResponse> create(@Valid @RequestBody final CreateJobRequest request) {
        final CreateJobResult result = jobCommandService.create(request);
        return result.created()
                ? ResponseEntity.created(URI.create("/api/v1/jobs/" + result.response().id())).body(result.response())
                : ResponseEntity.ok(result.response());
    }

    /**
     * Lists jobs newest first, optionally filtered.
     *
     * @param status status filter.
     * @param priority priority filter.
     * @param type job type filter, case-insensitive.
     * @param createdFrom inclusive ISO-8601 lower bound of the creation time.
     * @param createdTo exclusive ISO-8601 upper bound of the creation time.
     * @param page zero-based page index.
     * @param size page size, 1 to 100.
     * @return page of jobs.
     */
    @GetMapping
    public JobPageResponse list(
            @RequestParam(required = false) final JobStatus status,
            @RequestParam(required = false) final JobPriority priority,
            @RequestParam(required = false) final String type,
            @RequestParam(required = false) final Instant createdFrom,
            @RequestParam(required = false) final Instant createdTo,
            @RequestParam(defaultValue = "0") @Min(0) final int page,
            @RequestParam(defaultValue = "20") @Min(1) @Max(100) final int size) {
        return jobCommandService.list(status, priority, type, createdFrom, createdTo, page, size);
    }

    /**
     * Reads a job by id.
     *
     * @param id job id.
     * @return job response.
     */
    @GetMapping("/{id}")
    public JobResponse get(@PathVariable final UUID id) {
        return jobCommandService.get(id);
    }

    /**
     * Reads the attempt history of a job.
     *
     * @param id job id.
     * @return attempts, oldest first.
     */
    @GetMapping("/{id}/attempts")
    public List<JobAttemptResponse> attempts(@PathVariable final UUID id) {
        return jobCommandService.attempts(id);
    }

    /**
     * Reads a job by idempotency key.
     *
     * @param key idempotency key.
     * @return job response.
     */
    @GetMapping("/idempotency/{key}")
    public JobResponse getByIdempotencyKey(@PathVariable final String key) {
        return jobCommandService.getByIdempotencyKey(key);
    }

    /**
     * Replays a job currently in DLQ.
     *
     * @param id job id.
     * @param request replay request.
     * @return updated job response.
     */
    @PostMapping("/{id}/replay")
    public JobResponse replay(@PathVariable final UUID id, @Valid @RequestBody final ReplayJobRequest request) {
        return jobCommandService.replayDlq(id, request.priority() == null ? JobPriority.LOW : request.priority());
    }

    /**
     * Returns job counters grouped by status and by priority.
     *
     * @return stats response.
     */
    @GetMapping("/stats")
    public JobStatsResponse stats() {
        return jobCommandService.stats();
    }
}
