package com.github.pants.jobqueue.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.pants.jobqueue.support.IntegrationTestSupport;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * A bundled dashboard (here a stand-in {@code static/index.html}) is served for its page routes,
 * while API, actuator and asset paths keep their own behaviour.
 */
class DashboardFallbackIntegrationTest extends IntegrationTestSupport {

    private static final String MARKER = "dashboard-test-marker";

    @ParameterizedTest
    @ValueSource(strings = {"/", "/jobs", "/jobs/3f2c9a1e-0000-0000-0000-000000000000", "/queue", "/api"})
    void dashboardPagesGetIndexHtml(final String path) {
        final HttpResponse<String> response = get(path);

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains(MARKER);
    }

    @ParameterizedTest
    @ValueSource(strings = {"/api/v1/does-not-exist", "/actuator/does-not-exist", "/assets/missing.js"})
    void apiActuatorAndMissingFilesStay404(final String path) {
        final HttpResponse<String> response = get(path);

        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(response.body()).doesNotContain(MARKER);
    }

    @Test
    void apiRoutesStillWinOverTheFallback() {
        final HttpResponse<String> response = get("/api/v1/jobs/stats");

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("countsByStatus").doesNotContain(MARKER);
    }
}
