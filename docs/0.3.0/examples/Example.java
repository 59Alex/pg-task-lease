import io.github.alex59.pgtasklease.PgTaskQueue;
import io.github.alex59.pgtasklease.TaskLease;
import io.github.alex59.pgtasklease.TaskRequest;
import io.github.alex59.pgtasklease.TaskState;
import io.r2dbc.spi.ConnectionFactories;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Finite command-line demonstration. Apply schema.sql once before running. */
public final class Example {
    private static final Duration WAIT = Duration.ofSeconds(10);
    private static final Duration LEASE_TIME = Duration.ofSeconds(30);

    public static void main(String[] args) {
        String url = System.getenv("TASKS_R2DBC_URL");
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("Set TASKS_R2DBC_URL to a disposable PostgreSQL database");
        }
        var queue = new PgTaskQueue(ConnectionFactories.get(url));
        String name = "docs-" + UUID.randomUUID();
        var first = request(name, 10, 2);
        var retry = request(name, 0, 2);

        expect(queue.submit(first).block(WAIT), "first submission");
        expect(!queue.submit(first).block(WAIT), "duplicate submission must be ignored");
        expect(queue.submit(retry).block(WAIT), "second task submission");
        System.out.println("submit: inserted=true, duplicate=false");

        List<TaskLease> claimed = Flux.merge(
                        queue.claim(name, "worker-a", 1, LEASE_TIME),
                        queue.claim(name, "worker-b", 1, LEASE_TIME))
                .flatMapIterable(batch -> batch).collectList().block(WAIT);
        expect(claimed.size() == 2, "both workers must claim one task");
        expect(claimed.stream().map(TaskLease::id).distinct().count() == 2,
                "concurrent claims must not overlap");
        System.out.println("claim: 2 workers, 2 distinct tasks");

        var firstLease = claimed.stream().filter(lease -> lease.id().equals(first.id())).findFirst().orElseThrow();
        var retryLease = claimed.stream().filter(lease -> lease.id().equals(retry.id())).findFirst().orElseThrow();
        expect(queue.heartbeat(firstLease, Duration.ofSeconds(60)).block(WAIT), "heartbeat");
        expect(queue.complete(firstLease).block(WAIT), "completion");
        expect(!queue.complete(firstLease).block(WAIT), "repeated completion must not transition again");
        expect(queue.find(first.id()).block(WAIT).state() == TaskState.DONE, "durable completion");
        System.out.println("heartbeat: true; complete: true, then false; state=DONE");

        expect(queue.fail(retryLease, Duration.ZERO, "temporary demonstration failure").block(WAIT), "retry release");
        TaskLease next = claimOne(queue, name, LEASE_TIME);
        expect(next.id().equals(retry.id()), "same task must be retried");
        expect(next.attempt() == 2 && next.generation() == retryLease.generation() + 1, "next attempt and generation");
        expect(!next.token().equals(retryLease.token()), "fresh lease token");
        expect(!queue.complete(retryLease).block(WAIT), "stale capability must be rejected");
        expect(queue.fail(next, Duration.ZERO, "demonstration attempt budget exhausted").block(WAIT), "terminal failure");
        expect(queue.find(retry.id()).block(WAIT).state() == TaskState.DEAD, "bounded retries");
        System.out.println("retry: attempt=2, new token; stale complete=false; state=DEAD");

        var recoverable = request(name, 0, 2);
        expect(queue.submit(recoverable).block(WAIT), "recoverable task submission");
        TaskLease expired = claimOne(queue, name, Duration.ofMillis(100));
        // Finite demonstration wait; production recovery is scheduled by the application.
        Mono.delay(Duration.ofMillis(250)).block(WAIT);
        expect(!queue.complete(expired).block(WAIT), "expired completion must fail");
        expect(queue.recoverExpired(name, 10, Duration.ZERO).block(WAIT) == 1, "one expired lease recovered");
        TaskLease recovered = claimOne(queue, name, LEASE_TIME);
        expect(recovered.id().equals(recoverable.id()) && recovered.attempt() == 2, "recovered attempt");
        expect(queue.complete(recovered).block(WAIT), "recovered completion");
        System.out.println("recovery: 1 expired lease requeued; attempt=2; state=DONE");

        expect(queue.claim(name, "final-check", 1, LEASE_TIME).block(WAIT).isEmpty(), "queue drained");
        System.out.println("finished: 2 DONE, 1 DEAD; no ready tasks");
    }

    private static TaskRequest request(String name, int priority, int maxAttempts) {
        return new TaskRequest(UUID.randomUUID(), name, "{\"kind\":\"demo\"}", priority, maxAttempts, Duration.ZERO);
    }

    private static TaskLease claimOne(PgTaskQueue queue, String name, Duration leaseTime) {
        List<TaskLease> batch = queue.claim(name, "worker-retry", 1, leaseTime).block(WAIT);
        expect(batch.size() == 1, "exactly one eligible task");
        return batch.get(0);
    }

    private static void expect(boolean condition, String explanation) {
        if (!condition) throw new IllegalStateException("Example assertion failed: " + explanation);
    }
}
