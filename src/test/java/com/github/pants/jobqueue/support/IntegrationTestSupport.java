package com.github.pants.jobqueue.support;

import com.github.pants.jobqueue.service.JobCommandService;
import com.github.pants.jobqueue.service.JobExecutor;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * Boots the whole application on a real port against real PostgreSQL, Redis, and Kafka.
 *
 * <p>Containers are started once per JVM and every subclass shares one Spring context,
 * so the expensive setup runs a single time for the whole suite. Retry delays and the
 * outbox poll interval are shortened so lifecycle tests finish in seconds.</p>
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "app.job-queue.kafka.topic-partitions=2",
                "app.job-queue.retry.first-delay=1s",
                "app.job-queue.retry.second-delay=1s",
                "app.job-queue.retry.third-delay=1s",
                "app.job-queue.outbox.fixed-delay-ms=200",
                "app.job-queue.cors.allowed-origins=https://dashboard.example.test"
        })
public abstract class IntegrationTestSupport {

    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final GenericContainer<?> REDIS =
            new GenericContainer<>("redis:8-alpine").withExposedPorts(6379);
    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.0.0");

    static {
        Startables.deepStart(POSTGRES, REDIS, KAFKA).join();
    }

    private final HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    @LocalServerPort
    private int port;

    @Autowired
    protected ObjectMapper objectMapper;

    @Autowired
    protected JdbcClient jdbcClient;

    /** Spied (real behavior) so tests can observe which thread runs a job. */
    @MockitoSpyBean
    protected JobExecutor jobExecutor;

    /** Spied (real behavior) so tests can observe which thread serves an HTTP request. */
    @MockitoSpyBean
    protected JobCommandService jobCommandService;

    @DynamicPropertySource
    static void containerProperties(final DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        registry.add("app.job-queue.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    /**
     * Every test gets its own idempotency-key namespace, so tests never see each other's jobs.
     */
    @BeforeEach
    void newKeyNamespace() {
        keyPrefix = "it-" + UUID.randomUUID() + "-";
    }

    private String keyPrefix;

    /**
     * Returns an idempotency key unique to the running test.
     *
     * @param name readable suffix.
     * @return unique key.
     */
    protected String key(final String name) {
        return keyPrefix + name;
    }

    /**
     * Sends a JSON POST to the running server.
     *
     * @param path request path.
     * @param body JSON body.
     * @return response.
     */
    protected HttpResponse<String> post(final String path, final String body) {
        return send(HttpRequest.newBuilder(uri(path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build());
    }

    /**
     * Sends a GET to the running server.
     *
     * @param path request path and query.
     * @return response.
     */
    protected HttpResponse<String> get(final String path) {
        return send(HttpRequest.newBuilder(uri(path)).GET().build());
    }

    /**
     * Sends an arbitrary request to the running server.
     *
     * @param request request builder already pointed at {@link #uri(String)}.
     * @return response.
     */
    protected HttpResponse<String> send(final HttpRequest request) {
        try {
            return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (java.io.IOException exception) {
            throw new java.io.UncheckedIOException(exception);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(exception);
        }
    }

    /**
     * Builds an absolute URI on the running server.
     *
     * @param path request path and query.
     * @return URI.
     */
    protected URI uri(final String path) {
        return URI.create("http://localhost:" + port + path);
    }

    /**
     * Parses a JSON response body.
     *
     * @param response HTTP response.
     * @return JSON tree.
     */
    protected JsonNode json(final HttpResponse<String> response) {
        return objectMapper.readTree(response.body());
    }

    /**
     * Builds a create-job request body.
     *
     * @param idempotencyKey idempotency key.
     * @param priority HIGH or LOW.
     * @param payloadJson payload as a JSON object literal.
     * @param maxAttempts maximum attempts.
     * @return JSON body.
     */
    protected String createBody(
            final String idempotencyKey, final String priority, final String payloadJson, final int maxAttempts) {
        return """
                {"idempotencyKey":"%s","type":"demo_job","priority":"%s","payload":%s,"maxAttempts":%d}
                """.formatted(idempotencyKey, priority, payloadJson, maxAttempts);
    }

    /**
     * Creates a job through the API and returns its id.
     *
     * @param idempotencyKey idempotency key.
     * @param priority HIGH or LOW.
     * @param payloadJson payload JSON object literal.
     * @param maxAttempts maximum attempts.
     * @return created job id.
     */
    protected UUID createJob(
            final String idempotencyKey, final String priority, final String payloadJson, final int maxAttempts) {
        final HttpResponse<String> response = post(
                "/api/v1/jobs", createBody(idempotencyKey, priority, payloadJson, maxAttempts));
        if (response.statusCode() != 201) {
            throw new IllegalStateException("Expected 201 but got " + response.statusCode() + ": " + response.body());
        }
        return UUID.fromString(json(response).path("id").asString());
    }

    /**
     * Reads the durable status of a job straight from the database.
     *
     * @param jobId job id.
     * @return status.
     */
    protected String statusOf(final UUID jobId) {
        return jdbcClient.sql("SELECT status FROM jobs WHERE id = :id").param("id", jobId).query(String.class).single();
    }
}
