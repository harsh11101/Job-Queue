package com.github.pants.jobqueue.service;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import org.springframework.stereotype.Component;

/**
 * Stable process-level worker identity used in leases and logs.
 */
@Component
public class WorkerIdentity {

    private final String value;

    /**
     * Creates a worker identity from host, JVM process, and random suffix.
     */
    public WorkerIdentity() {
        this.value = hostname() + ":" + ManagementFactory.getRuntimeMXBean().getName() + ":" + UUID.randomUUID();
    }

    /**
     * Returns this process worker id.
     *
     * @return worker id.
     */
    public String value() {
        return value;
    }

    private String hostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException exception) {
            return "unknown-host";
        }
    }
}
