package com.github.pants.jobqueue.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.pants.jobqueue.support.IntegrationTestSupport;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

/**
 * HTTP contract tests: idempotency under concurrency, listing, stats, and error mapping.
 */
class JobApiIntegrationTest extends IntegrationTestSupport {

    /**
     * Many concurrent submissions of one request create exactly one job and one outbox
     * event; every caller gets the same job id, one with 201 and the rest with 200.
     */
    @Test
    void concurrentDuplicateSubmissionsCreateOneJob() throws Exception {
        final String key = key("concurrent");
        final String body = createBody(key, "HIGH", "{\"n\":1}", 4);
        final int callers = 24;

        final List<HttpResponse<String>> responses = new ArrayList<>();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            final List<Future<HttpResponse<String>>> futures = new ArrayList<>();
            for (int i = 0; i < callers; i++) {
                futures.add(executor.submit(() -> post("/api/v1/jobs", body)));
            }
            for (final Future<HttpResponse<String>> future : futures) {
                responses.add(future.get());
            }
        }

        assertThat(responses).filteredOn(r -> r.statusCode() == 201).hasSize(1);
        assertThat(responses).filteredOn(r -> r.statusCode() == 200).hasSize(callers - 1);
        assertThat(responses.stream().map(r -> json(r).path("id").asString()).distinct()).hasSize(1);

        final UUID jobId = UUID.fromString(json(responses.getFirst()).path("id").asString());
        assertThat(count("SELECT COUNT(*) FROM jobs WHERE idempotency_key = '" + key + "'")).isEqualTo(1);
        assertThat(count("SELECT COUNT(*) FROM job_outbox WHERE job_id = '" + jobId + "'")).isEqualTo(1);
    }

    /**
     * Reusing a key with a different payload is a conflict, not a silent overwrite.
     */
    @Test
    void reusingKeyWithDifferentRequestIsConflict() {
        final String key = key("conflict");
        createJob(key, "LOW", "{\"a\":1}", 4);

        final HttpResponse<String> response = post("/api/v1/jobs", createBody(key, "LOW", "{\"a\":2}", 4));

        assertThat(response.statusCode()).isEqualTo(409);
        assertThat(json(response).path("code").asString()).isEqualTo("IDEMPOTENCY_CONFLICT");
    }

    /**
     * Lookup by idempotency key finds the job; an unknown key is 404.
     */
    @Test
    void idempotencyKeyLookup() {
        final String key = key("lookup");
        final UUID jobId = createJob(key, "LOW", "{}", 4);

        assertThat(json(get("/api/v1/jobs/idempotency/" + key)).path("id").asString()).isEqualTo(jobId.toString());

        final HttpResponse<String> missing = get("/api/v1/jobs/idempotency/" + key("missing"));
        assertThat(missing.statusCode()).isEqualTo(404);
        assertThat(json(missing).path("code").asString()).isEqualTo("JOB_NOT_FOUND");
    }

    /**
     * Unknown job ids are 404 for both the job and its attempts.
     */
    @Test
    void unknownJobIsNotFound() {
        final UUID unknown = UUID.randomUUID();

        assertThat(get("/api/v1/jobs/" + unknown).statusCode()).isEqualTo(404);
        assertThat(get("/api/v1/jobs/" + unknown + "/attempts").statusCode()).isEqualTo(404);
    }

    /**
     * The list endpoint filters by priority, type, and creation window, and pages newest first.
     */
    @Test
    void listFiltersAndPaginates() {
        final Instant before = Instant.now().minusSeconds(1);
        final String type = "LIST_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12).toUpperCase();
        final List<UUID> created = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            final HttpResponse<String> response = post("/api/v1/jobs", """
                    {"idempotencyKey":"%s","type":"%s","priority":"%s","payload":{"i":%d}}
                    """.formatted(key("list-" + i), type.toLowerCase(), i % 2 == 0 ? "HIGH" : "LOW", i));
            assertThat(response.statusCode()).isEqualTo(201);
            created.add(UUID.fromString(json(response).path("id").asString()));
        }

        final JsonNode firstPage = json(get("/api/v1/jobs?type=" + type + "&size=2&page=0&createdFrom=" + before));
        assertThat(firstPage.path("totalElements").asLong()).isEqualTo(5);
        assertThat(firstPage.path("totalPages").asInt()).isEqualTo(3);
        assertThat(firstPage.path("items")).hasSize(2);
        assertThat(firstPage.path("items").get(0).path("id").asString()).isEqualTo(created.get(4).toString());

        final JsonNode lastPage = json(get("/api/v1/jobs?type=" + type + "&size=2&page=2"));
        assertThat(lastPage.path("items")).hasSize(1);
        assertThat(lastPage.path("items").get(0).path("id").asString()).isEqualTo(created.getFirst().toString());

        final JsonNode high = json(get("/api/v1/jobs?type=" + type + "&priority=HIGH"));
        assertThat(high.path("totalElements").asLong()).isEqualTo(3);

        final JsonNode none = json(get("/api/v1/jobs?type=" + type + "&createdTo=" + before));
        assertThat(none.path("totalElements").asLong()).isZero();
    }

    /**
     * Invalid enum values, page sizes, and timestamps are 400 with the stable error body.
     */
    @Test
    void invalidListParametersAreBadRequest() {
        for (final String query : List.of("status=NOPE", "priority=MEDIUM", "size=0", "size=101", "page=-1",
                "createdFrom=yesterday")) {
            final HttpResponse<String> response = get("/api/v1/jobs?" + query);
            assertThat(response.statusCode()).as(query).isEqualTo(400);
            assertThat(json(response).path("code").asString()).as(query).isEqualTo("BAD_REQUEST");
        }
    }

    /**
     * Malformed or invalid create bodies are 400.
     */
    @Test
    void invalidCreateBodyIsBadRequest() {
        assertThat(post("/api/v1/jobs", "{not json").statusCode()).isEqualTo(400);
        assertThat(post("/api/v1/jobs", "{\"type\":\"X\",\"priority\":\"HIGH\",\"payload\":{}}").statusCode())
                .isEqualTo(400);
        assertThat(post("/api/v1/jobs", createBody(key("attempts"), "HIGH", "{}", 21)).statusCode())
                .isEqualTo(400);
    }

    /**
     * Stats expose counts by status and by priority that include newly created jobs.
     */
    @Test
    void statsIncludeCountsByPriority() {
        createJob(key("stats-high"), "HIGH", "{}", 4);
        createJob(key("stats-low"), "LOW", "{}", 4);

        final JsonNode stats = json(get("/api/v1/jobs/stats"));

        assertThat(stats.path("countsByStatus").isObject()).isTrue();
        assertThat(sum(stats.path("countsByPriority").path("HIGH"))).isPositive();
        assertThat(sum(stats.path("countsByPriority").path("LOW"))).isPositive();
        assertThat(sum(stats.path("countsByStatus")))
                .isEqualTo(sum(stats.path("countsByPriority").path("HIGH"))
                        + sum(stats.path("countsByPriority").path("LOW")));
    }

    /**
     * The configured dashboard origin passes the CORS preflight; other origins do not.
     */
    @Test
    void corsAllowsOnlyConfiguredOrigin() {
        final HttpResponse<String> allowed = preflight("https://dashboard.example.test");
        assertThat(allowed.statusCode()).isEqualTo(200);
        assertThat(allowed.headers().firstValue("Access-Control-Allow-Origin"))
                .hasValue("https://dashboard.example.test");

        assertThat(preflight("https://evil.example.test").statusCode()).isEqualTo(403);
    }

    /**
     * The dashboard's health indicator reads the actuator from the browser, so it needs CORS too.
     */
    @Test
    void actuatorHealthAllowsConfiguredOrigin() {
        final HttpResponse<String> response = send(HttpRequest.newBuilder(uri("/actuator/health"))
                .header("Origin", "https://dashboard.example.test")
                .GET()
                .build());

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.headers().firstValue("Access-Control-Allow-Origin"))
                .hasValue("https://dashboard.example.test");
    }

    private HttpResponse<String> preflight(final String origin) {
        return send(HttpRequest.newBuilder(uri("/api/v1/jobs"))
                .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
                .header("Origin", origin)
                .header("Access-Control-Request-Method", "POST")
                .header("Access-Control-Request-Headers", "Content-Type")
                .build());
    }

    private long count(final String sql) {
        return jdbcClient.sql(sql).query(Long.class).single();
    }

    private static long sum(final JsonNode counts) {
        long total = 0;
        for (final JsonNode value : counts.values()) {
            total += value.asLong();
        }
        return total;
    }
}
