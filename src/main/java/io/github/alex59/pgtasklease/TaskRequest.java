package io.github.alex59.pgtasklease;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;

/**
 * A task to submit. Reusing its id makes retries of submission deduplicate.
 * @param id globally unique task id, retained for retries of submission
 * @param queue logical queue name, 1..120 characters
 * @param payload opaque non-null content
 * @param priority larger numbers are preferred at claim time
 * @param maxAttempts positive maximum number of claims
 * @param delay initial availability delay from database time
 */
public record TaskRequest(UUID id, String queue, String payload, int priority,
                          int maxAttempts, Duration delay) {
    /** Validates the request before any database operation. */
    public TaskRequest {
        Objects.requireNonNull(id, "id");
        Checks.text(queue, "queue", 120);
        Objects.requireNonNull(payload, "payload");
        if (maxAttempts < 1) throw new IllegalArgumentException("maxAttempts must be positive");
        Checks.millis(delay, "delay", false);
    }
}
