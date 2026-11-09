package com.github.pants.jobqueue.api;

import java.util.List;

/**
 * One page of jobs.
 *
 * @param items jobs on this page, newest first.
 * @param page zero-based page index.
 * @param size requested page size.
 * @param totalElements number of jobs matching the filter.
 * @param totalPages number of pages for the filter at this page size.
 */
public record JobPageResponse(
        List<JobResponse> items,
        int page,
        int size,
        long totalElements,
        int totalPages) {
}
