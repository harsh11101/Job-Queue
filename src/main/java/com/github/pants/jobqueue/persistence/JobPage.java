package com.github.pants.jobqueue.persistence;

import java.util.List;

/**
 * One page of jobs plus the number of jobs matching the filter across all pages.
 *
 * @param items jobs on this page.
 * @param totalElements total matching jobs.
 */
public record JobPage(List<JobRecord> items, long totalElements) {
}
