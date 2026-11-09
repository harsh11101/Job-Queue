package com.github.pants.jobqueue.api;

import java.time.Instant;

/**
 * Stable API error body used for domain validation errors.
 *
 * @param code application-level error code.
 * @param message safe human-readable message.
 * @param timestamp response creation timestamp.
 */
public record ErrorResponse(String code, String message, Instant timestamp) {
}
