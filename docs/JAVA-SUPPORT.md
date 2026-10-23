# Java support

[← Back to documentation and versions / Назад к документации и версиям](index.md)

Version 0.3.0 has a **Java 17 minimum**. The supported runtime matrix is **Java 17, 21, and 25 LTS**, plus **Java 27 (non-LTS)**. Production classes and test sources compile with `--release 17`, and the library does not require preview flags, reflection, or an alternative Java-specific implementation.

This supersedes the Java-27-only baseline of 0.2.0. The SQL schema, persisted task states, public method signatures, database-clock checks, token/generation fencing, and transaction boundaries remain unchanged. Existing task rows do not require a migration for this compatibility change.

## Build and runtime boundaries

| Concern | Policy |
|---|---|
| Compiler/API baseline | The selected JDK compiler uses `--release 17`; class-file major version 61. |
| Supported test runtimes | Actual JDK 17, 21, 25, and 27 launchers, selected with `-PtestJavaVersion`. |
| Production threading | Reactor and R2DBC remain non-blocking; no executor is installed by the library. |
| Newer JDK APIs | Not required by production classes. Callers can choose newer runtime capabilities in their own code. |
| Preview features | Not enabled or required. |

Records, text blocks, immutable collection copies, and explicit validation remain useful stable Java 17 tools. First-element access uses `get(0)` behind the existing empty-list guard instead of requiring the Java 21 sequenced-collection API. The missing-task result remains an empty `Mono`.

## Runtime-specific concurrency coverage

`independentWorkersCompleteEveryTaskWithoutDuplicateOwnership` inserts 96 tasks and releases 12 worker clients from a shared start gate. Each claims a batch, heartbeats its leases, completes them, and repeats until there are no eligible tasks. The test checks unique ownership, correct attempt/generation values, and that every submitted task ends in `DONE` with one attempt.

| Test runtime | Worker execution |
|---|---|
| Java 17 | A fixed executor with 12 platform threads. |
| Java 21 | A virtual-thread-per-task executor; each worker is asserted to be a virtual thread. |
| Java 25 LTS | The same virtual-thread assertions and queue invariants. |
| Java 27 (non-LTS) | The same virtual-thread assertions and queue invariants; the Java 17 bytecode/API baseline is retained. |

The executor factory and `Thread.isVirtual()` are accessed reflectively **only in test code** on Java 21/25/27. This permits the tests to compile with the Java 17 API baseline while exercising real virtual threads on newer runtimes. Reflection failure fails the test; there is no silent fallback to platform threads. Executors are terminated explicitly in `finally`, which also works on Java 17 where `ExecutorService` is not `AutoCloseable`.

The other reactive-concurrency, retry, expiry, stale-capability, and lock-wait tests run on every supported JDK. None is skipped simply because virtual threads are unavailable. This is correctness coverage, not a throughput benchmark or a proof of exactly-once external effects.

The 0.3.0 local matrix was verified against PostgreSQL 17 with 16 tests passing and none skipped on each runtime: Temurin 17.0.21+9, Microsoft 21.0.9+10-LTS, Oracle 25.0.1+8-LTS-27, and Oracle 27.0.1.0.2. The test output records platform workers on 17 and virtual workers on 21/25/27. JAR classes use major version 61 with no preview marker, and Gradle publication metadata declares JVM 17.

## Run the matrix locally

Install JDK 17 plus the JDKs you want to test and make them discoverable by Gradle toolchains. Start the disposable PostgreSQL described in the README, then run these commands sequentially against it:

```sh
PG_TASK_TEST_URL=r2dbc:postgresql://localhost:55432/pg_task_lease_test ./gradlew clean build -PtestJavaVersion=17
PG_TASK_TEST_URL=r2dbc:postgresql://localhost:55432/pg_task_lease_test ./gradlew test -PtestJavaVersion=21
PG_TASK_TEST_URL=r2dbc:postgresql://localhost:55432/pg_task_lease_test ./gradlew test -PtestJavaVersion=25
PG_TASK_TEST_URL=r2dbc:postgresql://localhost:55432/pg_task_lease_test ./gradlew test -PtestJavaVersion=27
```

Changing `testJavaVersion` selects the compiler and test JVM; compilation still targets the Java 17 API and bytecode. The integration test reports the actual runtime version and worker mode in its test output. Each run recreates the test table, so do not run matrix entries simultaneously against the same database. CI uses isolated PostgreSQL instances per matrix job.

## Application responsibilities

Virtual threads do not increase database connection capacity or change lease semantics. Bound handler concurrency and size the pool deliberately. Do not call `.block()` from a Reactor event-loop callback. The imperative concurrency test waits only in its independent caller threads; production database I/O stays asynchronous.

Thread-local or scoped values are not used to carry lease ownership across reactive callbacks. Tokens and generations remain explicit in `TaskLease`; Reactor `Context` is the appropriate application mechanism for subscription-scoped metadata. The selected JDK does not make external effects exactly once or remove the need for recovery and idempotent handlers.

The library depends on Reactor Core and R2DBC SPI; applications own their PostgreSQL driver and network dependency policy. Validate dependency behavior on the selected JDK rather than inferring it from bytecode compatibility alone.

The tested PostgreSQL R2DBC 1.1.3.RELEASE dependency graph includes Netty 4.1.112.Final, which emits `sun.misc.Unsafe` and native-access warnings on the tested JDK 25 and 27. The matrix passes without suppressing those warnings. This does not promise compatibility with future JDK API removals.

## References

- Java 17 API: [ExecutorService](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/concurrent/ExecutorService.html).
- Java 21 API: [Executors](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/Executors.html#newVirtualThreadPerTaskExecutor()).
- Oracle: [Virtual threads](https://docs.oracle.com/en/java/javase/25/core/virtual-threads.html).

[← Back to documentation and versions / Назад к документации и версиям](index.md)
