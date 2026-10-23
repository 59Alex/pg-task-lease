# pg-task-lease 0.3.0 — usage guide

[English](en.md) · [Русский](ru.md)

[Versions](../README.md) · [README](../../README.md) · [Java features](java.en.md)

Reactive PostgreSQL task leasing: atomic claims, explicit ownership, bounded retries and recovery after worker failure.

## Capabilities at a glance

| Capability | Use case | Example |
| --- | --- | --- |
| 1. Install locally and prepare PostgreSQL | A small reactive client for explicit task ownership. Start with Java 17+, a PostgreSQL database, the library artifact, and one migration. | [Code and result](#1-install-locally-and-prepare-postgresql) |
| 2. Submit once and retry submission safely | Choose a stable task UUID before the first attempt. Reusing it prevents a retry of submission from creating another task. | [Code and result](#2-submit-once-and-retry-submission-safely) |
| 3. Claim batches with parallel workers | A claim atomically selects eligible rows and records ownership. Independent workers can compete for the same logical queue. | [Code and result](#3-claim-batches-with-parallel-workers) |
| 4. Complete only with current ownership | TaskLease carries a token and generation. Together with database expiry, they fence state changes from an old worker. | [Code and result](#4-complete-only-with-current-ownership) |
| 5. Renew long-running work | Heartbeat preserves current ownership for work that needs more time. Schedule it comfortably before expiry. | [Code and result](#5-renew-long-running-work) |
| 6. Retry, exhaust the budget, and recover expiry | Fail releases a current lease deliberately. Recovery handles workers that disappeared without releasing it. | [Code and result](#6-retry-exhaust-the-budget-and-recover-expiry) |
| 7. Own the application lifecycle and connection pool | The library coordinates task rows. Your application decides when to poll, run handlers, renew leases and recover abandoned work. | [Code and result](#7-own-the-application-lifecycle-and-connection-pool) |
| 8. Run the example and diagnose failures | The downloadable example is a finite workflow with assertions. It retains its rows and never drops tables or modifies your application schema. | [Code and result](#8-run-the-example-and-diagnose-failures) |

## 1. Install locally and prepare PostgreSQL

A small reactive client for explicit task ownership. Start with Java 17+, a PostgreSQL database, the library artifact, and one migration.

This guide describes local version 0.3.0. Java 17, 21 and 25 LTS plus Java 27 are tested. Maven Central publication is not configured: install from a checkout that contains 0.3.0 before using the dependency below.

```bash
cd pg-task-lease
./gradlew publishToMavenLocal -PtestJavaVersion=17
```

Expected: the library, POM, sources and Javadoc are installed in your local Maven repository. This command does not publish to a remote registry.

```kotlin
repositories { mavenLocal(); mavenCentral() }
dependencies {
    implementation("io.github.59alex:pg-task-lease:0.3.0")
    runtimeOnly("org.postgresql:r2dbc-postgresql:1.1.3.RELEASE")
}
```

Consumer dependencies. Reactor Core and R2DBC SPI come with the library; the PostgreSQL driver is selected by the application.

```bash
psql "$TASKS_PSQL_URL" -v ON_ERROR_STOP=1 -f schema.sql
```

Apply the downloadable schema.sql once through your migration process. Expected: one public.pg_task_lease table and two indexes. There is no DROP operation.

```java
import io.github.alex59.pgtasklease.*;
import io.r2dbc.spi.ConnectionFactories;
import java.time.Duration;
import java.util.UUID;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

var queue = new PgTaskQueue(
    ConnectionFactories.get(System.getenv("TASKS_R2DBC_URL")));
var wait = Duration.ofSeconds(10);
```

Place the imports before the class declaration and the initializers inside your application entry point or command-line main method. TASKS_R2DBC_URL contains the connection settings; do not hard-code production credentials.

> **How to read the examples**
>
> The following fragments illustrate independent operations and assume the queue client above. Lease-specific examples require a fresh, current lease; they are not a script to run in sequence. Download Example.java for a complete finite workflow. Blocking waits appear only at the command-line boundary, never inside a Reactor event-loop callback.

## 2. Submit once and retry submission safely

Choose a stable task UUID before the first attempt. Reusing it prevents a retry of submission from creating another task.

```java
var taskId = UUID.randomUUID();
var request = new TaskRequest(taskId, "mail", "{"recipientId":42}",
    10, 3, Duration.ZERO);

boolean inserted = queue.submit(request).block(wait);
boolean duplicate = queue.submit(request).block(wait);
System.out.println(inserted + ", " + duplicate);
```

Expected for a fresh UUID: true, false. The second call does not replace the payload, priority or attempt budget.

- TaskRequest fields: id, queue, payload, priority, maxAttempts, delay. Payload is an opaque non-null string; the library does not serialize business objects.
- UUIDs are global across all queues. Persist the UUID in the producer workflow; generating a new UUID for every transport retry defeats deduplication.
- A positive delay schedules availability from the database clock. Higher numeric priority is preferred; maxAttempts is the maximum number of claims, including claims followed by a crash.

> **Deduplication scope**
>
> Submission deduplication lasts while the task row exists. It does not deduplicate a handler’s external effects. Removing a terminal row also removes its submission record.

## 3. Claim batches with parallel workers

A claim atomically selects eligible rows and records ownership. Independent workers can compete for the same logical queue.

```java
var queueName = "claim-demo-" + UUID.randomUUID();
Flux.range(0, 2)
    .concatMap(i -> queue.submit(new TaskRequest(UUID.randomUUID(),
        queueName, "{"kind":"demo"}", i, 2, Duration.ZERO)))
    .then().block(wait);

var leases = Flux.merge(
        queue.claim(queueName, "worker-a", 1, Duration.ofSeconds(30)),
        queue.claim(queueName, "worker-b", 1, Duration.ofSeconds(30)))
    .flatMapIterable(batch -> batch)
    .collectList().block(wait);
long distinctTasks = leases.stream().map(TaskLease::id).distinct().count();
System.out.println(leases.size() + ", " + distinctTasks);
```

Expected with the two ready tasks above: 2, 2. Each claim returns an immutable list; an empty list means no eligible, unlocked task was found.

- Selection uses PostgreSQL FOR UPDATE SKIP LOCKED. A locked higher-priority task may be bypassed; completion order is not globally FIFO.
- Queue names isolate selection, not database permissions. Different application roles still need their own database access policy.
- Claim limits are 1–1000. Keep a batch small enough to start processing before its leases expire; local waiting also consumes lease time.

> **A subscription is an operation**
>
> Reactor publishers are cold. Subscribing again to the same claim publisher performs a new claim; it does not replay the previous list.

## 4. Complete only with current ownership

TaskLease carries a token and generation. Together with database expiry, they fence state changes from an old worker.

```java
boolean acknowledged = queue.complete(lease).block(wait);
boolean repeated = queue.complete(lease).block(wait);
TaskSnapshot snapshot = queue.find(lease.id()).block(wait);
System.out.println(acknowledged + ", " + repeated + ", " + snapshot.state());
```

For a current unexpired lease: true, false, DONE. A stale, expired or already completed lease returns false instead of changing the row again.

Call complete only after the handler’s successful result. The client checks state, token, generation and database time after obtaining the row lock. The diagnostic owner string is not the ownership credential.

> **External effects remain outside the transaction**
>
> A payment, email or remote write can succeed before the process crashes without acknowledging the task. A later attempt can repeat that effect. Use the task UUID as an idempotency key or a transaction supported by the target system; this library does not provide exactly-once external effects.

> **A lost response is ambiguous**
>
> A connection error after complete is not proof of rollback: PostgreSQL may have committed. Inspect state where useful. A false response to a repeated complete alone cannot distinguish an earlier successful acknowledgement from loss of ownership.

## 5. Renew long-running work

Heartbeat preserves current ownership for work that needs more time. Schedule it comfortably before expiry.

```java
boolean renewed = queue.heartbeat(lease, Duration.ofSeconds(60)).block(wait);
if (!renewed) {
    throw new IllegalStateException("Lease lost; stop work where possible");
}
TaskSnapshot current = queue.find(lease.id()).block(wait);
System.out.println(current.expiresAt());
```

For a current lease, expiry is at least database-now plus 60 seconds. A shorter requested duration never shortens an existing lease. Expired leases cannot be revived.

- TaskLease is an immutable acquisition snapshot. Its expiresAt value is not updated by heartbeat; find returns the current database snapshot.
- A false heartbeat means ownership could not be renewed. Stop cancellable work; cancellation cannot undo a request already accepted by an external system.
- Lease durations use whole milliseconds from 1 millisecond to 365 days. Choose a practical duration above expected handler, GC and network pauses; there is no universal safe default.

## 6. Retry, exhaust the budget, and recover expiry

Fail releases a current lease deliberately. Recovery handles workers that disappeared without releasing it.

```java
boolean released = queue.fail(lease, Duration.ofSeconds(5),
    "Temporary upstream timeout").block(wait);
TaskSnapshot current = queue.find(lease.id()).block(wait);
System.out.println(released + ", " + current.state());
```

If attempts remain: true, READY, with eligibility delayed by 5 seconds. On the last allowed attempt: true, DEAD. A stale lease returns false.

```java
int recovered = queue.recoverExpired("mail", 100,
    Duration.ofSeconds(5)).block(wait);
System.out.println(recovered);
```

Expected: 0–100 expired leases transitioned. Recoverable tasks become READY after the delay; tasks with exhausted claim budgets become DEAD.

- Attempts are counted on claim, not on the first line of your handler. A crash before handler execution still consumes an attempt.
- A re-claim increments generation and supplies a fresh token. An old worker cannot complete or fail the new lease.
- Recovery is bounded and safe to invoke from several processes. The application must schedule it; expiry by itself does not execute a recovery query.
- DEAD and DONE rows remain for inspection and retention policy. No automatic retry from DEAD, archive scheduler, or hidden infinite retry loop is installed.

> **Keep diagnostics safe**
>
> The error string is stored verbatim and is limited to 2048 characters. Remove secrets and unnecessary personal data before calling fail.

## 7. Own the application lifecycle and connection pool

The library coordinates task rows. Your application decides when to poll, run handlers, renew leases and recover abandoned work.

```java
Mono<Integer> oneRecoveryPass = queue.recoverExpired("mail", 100, Duration.ZERO);
int changed = oneRecoveryPass.block(wait);
System.out.println("Recovered in this pass: " + changed);
```

This finite pass performs one recovery operation and returns. Repetition, backoff and shutdown are explicit application choices.

- Each subscribed operation obtains one auto-commit connection and releases it on completion, failure or cancellation. A caller-owned pooled ConnectionFactory can be supplied to PgTaskQueue.
- Do not supply a transaction-bound connection factory. No transaction stays open while your handler executes, and submit is not atomic with arbitrary application writes.
- Use an outbox or another explicit transaction design when a domain change and task submission must commit together.
- Bound processing concurrency against the database pool and external services. Virtual threads on newer JDKs do not increase PostgreSQL connection capacity.
- Cancellation or a failed response can leave a committed lease without a usable result at the caller. Allow expiry and recovery to resolve it; do not invent a replacement token.

## 8. Run the example and diagnose failures

The downloadable example is a finite workflow with assertions. It retains its rows and never drops tables or modifies your application schema.

Download Example.java, pom.xml and schema.sql together. Requirements: Java 17+, Maven 3.8+, and the locally installed pg-task-lease 0.3.0 artifact. The following database is disposable and listens only on localhost. Its credentials are demonstration fixtures.

```bash
docker run --rm -d --name pg-lease-docs \
  -e POSTGRES_DB=pg_task_lease_demo -e POSTGRES_USER=lease_demo \
  -e POSTGRES_PASSWORD=lease_demo_only -p 127.0.0.1:55433:5432 postgres:17-alpine
until docker exec pg-lease-docs pg_isready -U lease_demo -d pg_task_lease_demo; do sleep 1; done
docker exec -i pg-lease-docs psql -U lease_demo -d pg_task_lease_demo \
  -v ON_ERROR_STOP=1 < schema.sql
export TASKS_R2DBC_URL='r2dbc:postgresql://lease_demo:lease_demo_only@localhost:55433/pg_task_lease_demo'
mvn -q compile dependency:build-classpath -Dmdep.outputFile=target/classpath.txt
java -cp "target/classes:$(cat target/classpath.txt)" Example
docker stop pg-lease-docs
```

Run from the downloaded example directory. Stop only the container you started; the final command removes this --rm demonstration container.

```text
submit: inserted=true, duplicate=false
claim: 2 workers, 2 distinct tasks
heartbeat: true; complete: true, then false; state=DONE
retry: attempt=2, new token; stale complete=false; state=DEAD
recovery: 1 expired lease requeued; attempt=2; state=DONE
finished: 2 DONE, 1 DEAD; no ready tasks
```

Expected output. The example checks queue transitions, not a real email/payment handler or exactly-once delivery.

- Dependency not found: version 0.3.0 is not published to Central. Install the matching checkout with publishToMavenLocal, then resolve the consumer dependencies again.
- relation "public.pg_task_lease" does not exist: apply schema.sql to the same database used by TASKS_R2DBC_URL. Do not drop an existing production table to fix a migration error.
- Empty claim: check the queue name, READY state, availableAt delay, attempt budget and concurrent locks. An empty list is a normal result, not a connection failure.
- complete, fail or heartbeat returns false: the capability is stale, expired, already released or terminal. Inspect find(id); do not bypass ownership checks.
- ConnectionFactory must supply auto-commit connections: use a standalone auto-commit factory or pool, not a connection bound to an application transaction.

> **Library test suite**
>
> The library suite is verified on Java 17/21/25/27 against real PostgreSQL. It includes 12 workers processing 96 tasks, stale-token checks and expiry during lock waits. Run it only against a disposable database ending in _test: the suite recreates its own table. The downloadable Example.java does not recreate tables.

## Downloadable examples

- [Example.java](examples/Example.java)
- [pom.xml](examples/pom.xml)
- [schema.sql](examples/schema.sql)

[MIT](../../LICENSE)
