package io.github.alex59.pgtasklease;

import java.time.Instant;
import java.util.UUID;

/**
 * An acquired task and its capability. Treat token and generation as an indivisible pair.
 * @param id task id
 * @param queue logical queue name
 * @param payload opaque task content
 * @param priority scheduling preference
 * @param attempt one-based claim count
 * @param maxAttempts configured claim budget
 * @param token unpredictable capability for this claim
 * @param generation monotonically increasing version while the task row is retained
 * @param owner diagnostic worker name
 * @param expiresAt database expiry at acquisition; heartbeat does not mutate this snapshot
 */
public record TaskLease(UUID id, String queue, String payload, int priority, int attempt,
                        int maxAttempts, UUID token, long generation, String owner,
                        Instant expiresAt) { }
