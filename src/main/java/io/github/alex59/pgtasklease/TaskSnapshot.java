package io.github.alex59.pgtasklease;

import java.time.Instant;
import java.util.UUID;

/**
 * Read-only inspection; it does not grant ownership of a task. Nullable lease fields indicate no lease.
 * @param id task id
 * @param queue logical queue name
 * @param state current durable state
 * @param attempts number of claims so far
 * @param maxAttempts configured claim budget
 * @param generation version of the most recent claim
 * @param availableAt earliest time a READY task may be claimed
 * @param expiresAt lease expiry, or null outside LEASED
 * @param owner diagnostic worker name, or null outside LEASED
 * @param lastError most recent failure description, or null
 */
public record TaskSnapshot(UUID id, String queue, TaskState state, int attempts, int maxAttempts,
                           long generation, Instant availableAt, Instant expiresAt, String owner,
                           String lastError) { }
