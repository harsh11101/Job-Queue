package com.github.pants.jobqueue.domain;

/**
 * Job priority mapped to independent Kafka topics, allowing high-priority workers
 * to scale independently from low-priority workers.
 */
public enum JobPriority {
    /** High-priority job routed to the hot lane. */
    HIGH,
    /** Low-priority job routed to the default/background lane. */
    LOW
}
