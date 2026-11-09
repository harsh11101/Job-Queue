package com.github.pants.jobqueue.util;

import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Provides injectable time source for deterministic tests and consistent UTC timestamps.
 */
@Configuration
public class ClockProvider {

    /**
     * Creates the default UTC clock.
     *
     * @return system UTC clock.
     */
    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
