package com.github.pants.jobqueue.config;

import java.io.IOException;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.servlet.resource.PathResourceResolver;

/**
 * Spring MVC configuration: lets the configured browser origins (the dashboard) call the API,
 * and serves a bundled dashboard when the image includes one (see {@code Dockerfile.demo}).
 */
@Configuration
@RequiredArgsConstructor
public class WebConfiguration implements WebMvcConfigurer {

    private final JobQueueProperties properties;

    /**
     * Serves files from {@code classpath:/static/} (the bundled dashboard). The dashboard routes
     * its pages in the browser, so a page address such as {@code /jobs/123} has no file behind it:
     * those requests get {@code index.html}. API and actuator paths, and missing files with an
     * extension (scripts, images), still get a normal 404. Without a bundled dashboard there is no
     * {@code index.html}, and every unknown path is a 404 as before.
     */
    @Override
    public void addResourceHandlers(final ResourceHandlerRegistry registry) {
        registry.addResourceHandler("/**")
                .addResourceLocations("classpath:/static/")
                .resourceChain(true)
                .addResolver(new PathResourceResolver() {
                    @Override
                    protected Resource getResource(final String path, final Resource location) throws IOException {
                        if (!path.isEmpty()) {
                            final Resource file = location.createRelative(path);
                            if (file.exists() && file.isReadable()) {
                                return file;
                            }
                        }
                        if (isApiPath(path) || path.substring(path.lastIndexOf('/') + 1).contains(".")) {
                            return null;
                        }
                        final Resource index = location.createRelative("index.html");
                        return index.exists() && index.isReadable() ? index : null;
                    }
                });
    }

    private static boolean isApiPath(final String path) {
        return path.startsWith("api/v1/") || path.equals("api/v1") || path.startsWith("actuator");
    }

    @Override
    public void addCorsMappings(final CorsRegistry registry) {
        registry.addMapping("/api/**")
                .allowedOrigins(properties.getCors().getAllowedOrigins().toArray(String[]::new))
                .allowedMethods("GET", "POST")
                .allowedHeaders("Content-Type")
                .exposedHeaders("Location")
                .maxAge(3600);
    }
}
