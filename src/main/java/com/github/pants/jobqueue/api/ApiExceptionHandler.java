package com.github.pants.jobqueue.api;

import com.github.pants.jobqueue.exception.IdempotencyConflictException;
import com.github.pants.jobqueue.exception.InvalidJobStateException;
import com.github.pants.jobqueue.exception.JobNotFoundException;
import java.time.Instant;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

/**
 * Centralized API exception mapping that returns stable error codes.
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    /**
     * Handles missing jobs.
     *
     * @param exception exception.
     * @return 404 response.
     */
    @ExceptionHandler(JobNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleNotFound(final JobNotFoundException exception) {
        return error(HttpStatus.NOT_FOUND, "JOB_NOT_FOUND", exception.getMessage());
    }

    /**
     * Handles idempotency conflicts.
     *
     * @param exception exception.
     * @return 409 response.
     */
    @ExceptionHandler(IdempotencyConflictException.class)
    public ResponseEntity<ErrorResponse> handleIdempotencyConflict(final IdempotencyConflictException exception) {
        return error(HttpStatus.CONFLICT, "IDEMPOTENCY_CONFLICT", exception.getMessage());
    }

    /**
     * Handles invalid state transitions.
     *
     * @param exception exception.
     * @return 409 response.
     */
    @ExceptionHandler(InvalidJobStateException.class)
    public ResponseEntity<ErrorResponse> handleInvalidState(final InvalidJobStateException exception) {
        return error(HttpStatus.CONFLICT, "INVALID_JOB_STATE", exception.getMessage());
    }

    /**
     * Handles malformed bodies, failed bean validation, and unparseable or out-of-range parameters.
     *
     * @param exception exception.
     * @return 400 response.
     */
    @ExceptionHandler({
            HttpMessageNotReadableException.class,
            MethodArgumentNotValidException.class,
            HandlerMethodValidationException.class,
            MethodArgumentTypeMismatchException.class,
            MissingServletRequestParameterException.class})
    public ResponseEntity<ErrorResponse> handleBadRequest(final Exception exception) {
        return error(HttpStatus.BAD_REQUEST, "BAD_REQUEST", "Request body or parameters are invalid");
    }

    private ResponseEntity<ErrorResponse> error(final HttpStatus status, final String code, final String message) {
        return ResponseEntity.status(status).body(new ErrorResponse(code, message, Instant.now()));
    }
}
