# pg-task-lease

Reactive PostgreSQL task leasing for workers that need explicit ownership, bounded retries, and recovery after a crash.

Submit a task, claim a short lease, process it, and acknowledge it with the same token and generation. An expired worker cannot acknowledge a task that another worker has reclaimed. The library uses R2DBC and Reactor; it does not require Spring or run a background scheduler.

## What it handles

- Atomic batch claims with `FOR UPDATE SKIP LOCKED` across independent workers.
- Queue isolation, priority preference, delayed availability, and bounded attempt counts.
- Heartbeats and fenced completion/failure using a lease token and monotonic generation.
- Recovery of expired leases, with exhausted tasks retained as `DEAD` for inspection.
- Idempotent submission by caller-provided task UUID.

This provides **at-least-once processing attempts within the configured retry budget**, assuming workers and the recovery loop keep running and PostgreSQL preserves committed writes. It does **not** make external effects exactly once. A worker can finish an external operation and crash before acknowledging it; handlers must deduplicate by task id or use a transactional/idempotent external API.

## Build and use

Version `0.2.0` requires **Java 27** for building and running. Use JDK 27 and the included Gradle wrapper; no preview flags are needed. It can be built and installed locally; Maven Central publication is not configured. See [Java 27 adoption](docs/JAVA-27.md) for the implemented changes and compatibility boundary.

```sh
./gradlew publishToMavenLocal
```

In the consuming build:

```kotlin
repositories { mavenLocal(); mavenCentral() }
dependencies {
    implementation("io.github.59alex:pg-task-lease:0.2.0")
    runtimeOnly("org.postgresql:r2dbc-postgresql:1.1.3.RELEASE")
}
```

Apply [the SQL migration](src/main/resources/pg-task-lease.sql) once to your PostgreSQL database with your normal migration tool. It creates `public.pg_task_lease` and two partial indexes. PostgreSQL 17 is integration-tested. The application role needs `SELECT`, `INSERT`, and `UPDATE` on this table; migration privileges are separate.

```java
import io.github.alex59.pgtasklease.*;
import io.r2dbc.spi.ConnectionFactories;
import java.time.Duration;
import java.util.UUID;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

var queue = new PgTaskQueue(ConnectionFactories.get(System.getenv("TASKS_R2DBC_URL")));
var request = new TaskRequest(UUID.randomUUID(), "notifications",
        "{\"recipientId\":42}", 10, 5, Duration.ZERO);

Mono<Boolean> submitted = queue.submit(request);

// Subscribe through your application's lifecycle. Do not leave these cold publishers unused.
Mono<Void> oneBatch = submitted.then(
    queue.claim("notifications", "worker-1", 4, Duration.ofSeconds(30))
        .flatMapMany(Flux::fromIterable)
        .flatMap(lease -> processIdempotently(lease.id(), lease.payload())
            .then(queue.complete(lease))
            .flatMap(acknowledged -> acknowledged ? Mono.<Void>empty()
                : Mono.error(new IllegalStateException("Lease was lost"))), 4)
        .then());
```

`processIdempotently(UUID, String)` is your handler returning `Mono<Void>`. Keep processing concurrency at least as large as the batch you claim, or ensure leases cover local waiting time. The example intentionally propagates handler errors: a failed or cancelled batch remains leased until recovery. For controlled retries, call `fail` when the handler fails; do not classify a transport error from `complete` as proof that the handler failed.

The [tested usage example](src/test/java/io/github/alex59/pgtasklease/PgTaskQueueIntegrationTest.java) exercises the API against PostgreSQL.

## Lifecycle contract

| Operation | Result and responsibility |
|---|---|
| `submit(request)` | `true` inserted, `false` UUID already present. Existing payload is never overwritten; UUIDs are global across queues. |
| `claim(queue, owner, limit, leaseTime)` | Immutable batch, or empty list. `owner` is diagnostic; token + generation enforce ownership. |
| `heartbeat(lease, leaseTime)` | `true` extends expiry to at least database-now + duration; never shortens or revives. |
| `complete(lease)` | `true` changes an unexpired current lease to `DONE`; `false` means it could not. |
| `fail(lease, retryDelay, error)` | `true` releases current ownership to `READY` or `DEAD`; `false` means ownership was lost. |
| `recoverExpired(queue, limit, retryDelay)` | Number recovered in this bounded batch; exhausted leases become `DEAD`. Run periodically in one or several processes. |
| `find(id)` | Diagnostic state snapshot; empty if absent. |

Durations use whole milliseconds, from zero (delays) or one millisecond (leases) through 365 days. Batches are limited to 1000. Higher numeric priority goes first, followed by creation time and UUID. Locks and parallel execution mean this is a preference, **not a global ordering guarantee**. Sustained high-priority work can starve low-priority work.

The returned lease is an immutable acquisition snapshot: a successful heartbeat updates PostgreSQL, not its `expiresAt()` field. Database time is authoritative. Choose a lease comfortably above ordinary processing/GC/network pauses and heartbeat well before expiry. A failed heartbeat must cause the application to stop work where possible; it cannot undo an already-issued external request.

Every subscribed operation acquires one auto-commit connection and closes/releases it on completion, error, or cancellation. Supply a connection pool in production. Do not supply a transaction-bound factory. A transport or connection-release error can occur after SQL has committed: an error is not proof of rollback. Publishers are cold: resubscribing to `claim` is a **new claim**, not replay of the old batch. Keep the supplied UUID stable when retrying submission.

No payload codecs, handler execution, automatic retry policy, polling, or retention scheduler are hidden inside the client. Payloads are opaque strings. Store compact references instead of large data. Diagnostic failure text is stored verbatim: remove secrets before passing it. Archive/delete terminal tasks with your own retention policy; deleting a task also removes its submission deduplication record.

## Run the integration suite

The suite requires a disposable database whose name ends in `_test`. It drops and recreates **only** `public.pg_task_lease` there. Missing PostgreSQL fails the build; integration tests are never silently skipped.

```sh
docker run --rm -d --name pg-task-lease-tests \
  -e POSTGRES_DB=pg_task_lease_test -e POSTGRES_USER=lease_test \
  -e POSTGRES_PASSWORD=lease_test_only -p 127.0.0.1:55432:5432 postgres:17-alpine

# Wait until pg_isready reports ready, then:
PG_TASK_TEST_URL=r2dbc:postgresql://localhost:55432/pg_task_lease_test ./gradlew build
docker stop pg-task-lease-tests
```

The credentials above are disposable test fixtures. Set `PG_TASK_TEST_USER` and `PG_TASK_TEST_PASSWORD` if your test instance uses different credentials.

Tests cover concurrent disjoint claims, queue/priority/delay behavior, duplicate submissions, retries and exhaustion, heartbeat extension, stale token/generation rejection, expiry recovery, and expiry while an acknowledgement waits on a row lock. A separate concurrency test starts 12 virtual-thread worker clients against PostgreSQL, processes 96 tasks, and checks unique ownership and durable completion. This is a correctness check, not a throughput benchmark. See [architecture and failure boundaries](docs/architecture.md).

## License

[MIT](LICENSE). Gradle wrapper files retain their upstream Apache-2.0 notices; the distribution's [license](gradle/licenses/LICENSE) and [notice](gradle/licenses/NOTICE) are included. Runtime dependencies keep their respective licenses.
