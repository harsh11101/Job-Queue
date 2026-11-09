package com.github.pants.jobqueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.github.pants.jobqueue.support.IntegrationTestSupport;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

/**
 * End-to-end job lifecycle through HTTP, PostgreSQL, the outbox, Kafka, and the workers.
 */
class JobLifecycleIntegrationTest extends IntegrationTestSupport {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);

    /**
     * A job flows API, outbox, Kafka, worker, SUCCEEDED, with one side-effect receipt.
     */
    @Test
    void createdJobIsProcessedToSucceeded() {
        final UUID jobId = createJob(key("success"), "HIGH", "{\"amount\":\"10.00\"}", 4);

        await().atMost(TIMEOUT).until(() -> "SUCCEEDED".equals(statusOf(jobId)));

        final JsonNode job = json(get("/api/v1/jobs/" + jobId));
        assertThat(job.path("status").asString()).isEqualTo("SUCCEEDED");
        assertThat(job.path("payloadJson").asString()).isEqualTo("{\"amount\":\"10.00\"}");
        assertThat(job.path("resultJson").asString()).contains("\"effectCreated\":true");
        assertThat(job.path("completedAt").isNull()).isFalse();

        final JsonNode attempts = json(get("/api/v1/jobs/" + jobId + "/attempts"));
        assertThat(attempts).hasSize(1);
        assertThat(attempts.get(0).path("status").asString()).isEqualTo("SUCCEEDED");
        assertThat(attempts.get(0).path("topic").asString()).isEqualTo("jobs-high");
        assertThat(receiptCount(jobId)).isEqualTo(1);
    }

    /**
     * Priority selects the Kafka lane the first outbox event targets.
     */
    @Test
    void priorityRoutesToItsOwnTopic() {
        final UUID high = createJob(key("high"), "HIGH", "{}", 4);
        final UUID low = createJob(key("low"), "LOW", "{}", 4);

        assertThat(firstOutboxTopic(high)).isEqualTo("jobs-high");
        assertThat(firstOutboxTopic(low)).isEqualTo("jobs-low");
    }

    /**
     * A retryable failure walks the retry topics, lands in DLQ after max attempts, and a
     * replay sends it through the chosen lane again with the next attempt number.
     */
    @Test
    void retryableFailureRetriesThenDeadLettersThenReplays() {
        final UUID jobId = createJob(key("retry"), "LOW", "{\"simulateRetryableFailure\":true}", 2);

        await().atMost(TIMEOUT).until(() -> "DLQ".equals(statusOf(jobId)));

        final JsonNode attempts = json(get("/api/v1/jobs/" + jobId + "/attempts"));
        assertThat(attempts).hasSize(2);
        assertThat(attempts.get(0).path("status").asString()).isEqualTo("RETRY_SCHEDULED");
        assertThat(attempts.get(0).path("topic").asString()).isEqualTo("jobs-low");
        assertThat(attempts.get(1).path("status").asString()).isEqualTo("DLQ");
        assertThat(attempts.get(1).path("topic").asString()).isEqualTo("jobs-retry-5s");
        assertThat(json(get("/api/v1/jobs/" + jobId)).path("failureClass").asString())
                .endsWith("RetryableJobException");

        final HttpResponse<String> replay = post("/api/v1/jobs/" + jobId + "/replay", "{\"priority\":\"HIGH\"}");
        assertThat(replay.statusCode()).isEqualTo(200);
        assertThat(json(replay).path("priority").asString()).isEqualTo("HIGH");

        await().atMost(TIMEOUT).until(() -> json(get("/api/v1/jobs/" + jobId + "/attempts")).size() == 3);
        await().atMost(TIMEOUT).until(() -> "DLQ".equals(statusOf(jobId)));
        final JsonNode third = json(get("/api/v1/jobs/" + jobId + "/attempts")).get(2);
        assertThat(third.path("attempt").asInt()).isEqualTo(3);
        assertThat(third.path("topic").asString()).isEqualTo("jobs-high");
    }

    /**
     * A non-retryable failure skips the retry lanes.
     */
    @Test
    void permanentFailureGoesStraightToDlq() {
        final UUID jobId = createJob(key("permanent"), "LOW", "{\"simulatePermanentFailure\":true}", 4);

        await().atMost(TIMEOUT).until(() -> "DLQ".equals(statusOf(jobId)));

        final JsonNode attempts = json(get("/api/v1/jobs/" + jobId + "/attempts"));
        assertThat(attempts).hasSize(1);
        assertThat(attempts.get(0).path("failureClass").asString()).endsWith("NonRetryableJobException");
    }

    /**
     * Replay is refused for a job that is not in DLQ.
     */
    @Test
    void dlqJobCanBeCancelledAndIsThenNeverReplayed() {
        final UUID jobId = createJob(key("cancel-dlq"), "LOW", "{\"simulatePermanentFailure\":true}", 4);
        await().atMost(TIMEOUT).until(() -> "DLQ".equals(statusOf(jobId)));

        final HttpResponse<String> cancel = post("/api/v1/jobs/" + jobId + "/cancel", "");
        assertThat(cancel.statusCode()).isEqualTo(200);
        assertThat(json(cancel).path("status").asString()).isEqualTo("CANCELLED");

        final HttpResponse<String> again = post("/api/v1/jobs/" + jobId + "/cancel", "");
        assertThat(again.statusCode()).isEqualTo(200);
        assertThat(json(again).path("status").asString()).isEqualTo("CANCELLED");

        final HttpResponse<String> replay = post("/api/v1/jobs/" + jobId + "/replay", "{\"priority\":\"HIGH\"}");
        assertThat(replay.statusCode()).isEqualTo(409);
        assertThat(statusOf(jobId)).isEqualTo("CANCELLED");
    }

    @Test
    void succeededJobCannotBeCancelled() {
        final UUID jobId = createJob(key("cancel-done"), "HIGH", "{}", 4);
        await().atMost(TIMEOUT).until(() -> "SUCCEEDED".equals(statusOf(jobId)));

        final HttpResponse<String> response = post("/api/v1/jobs/" + jobId + "/cancel", "");

        assertThat(response.statusCode()).isEqualTo(409);
        assertThat(json(response).path("code").asString()).isEqualTo("INVALID_JOB_STATE");
        assertThat(statusOf(jobId)).isEqualTo("SUCCEEDED");
    }

    @Test
    void cancellingUnknownJobIsNotFound() {
        final HttpResponse<String> response = post("/api/v1/jobs/" + UUID.randomUUID() + "/cancel", "");

        assertThat(response.statusCode()).isEqualTo(404);
    }

    @Test
    void replayOfNonDlqJobIsRejected() {
        final UUID jobId = createJob(key("not-dlq"), "LOW", "{}", 4);

        final HttpResponse<String> response = post("/api/v1/jobs/" + jobId + "/replay", "{\"priority\":\"LOW\"}");

        assertThat(response.statusCode()).isEqualTo(409);
        assertThat(json(response).path("code").asString()).isEqualTo("INVALID_JOB_STATE");
    }

    private long receiptCount(final UUID jobId) {
        return jdbcClient.sql("SELECT COUNT(*) FROM side_effect_receipts WHERE job_id = :id")
                .param("id", jobId).query(Long.class).single();
    }

    private String firstOutboxTopic(final UUID jobId) {
        final List<String> topics = jdbcClient.sql(
                        "SELECT topic FROM job_outbox WHERE job_id = :id ORDER BY created_at")
                .param("id", jobId).query(String.class).list();
        return topics.getFirst();
    }
}
