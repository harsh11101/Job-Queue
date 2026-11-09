package com.github.pants.jobqueue.api;

/**
 * Service result for a create-job command.
 *
 * @param response job representation returned to the caller.
 * @param created true when this request inserted a new job, false for idempotent replay.
 */
public record CreateJobResult(JobResponse response, boolean created) {
}
