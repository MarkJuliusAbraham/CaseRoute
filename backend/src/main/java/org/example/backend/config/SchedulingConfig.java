package org.example.backend.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Runs the outbox relay on a timer. */
@Configuration
@EnableScheduling
public class SchedulingConfig {
}
