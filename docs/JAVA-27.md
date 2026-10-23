# Java 27 adoption — historical 0.2.0 decision

[← Back to documentation and versions / Назад к документации и версиям](index.md)

> **Superseded in 0.3.0.** The minimum is Java 17, with supported LTS runtimes 17, 21, and 25, plus Java 27 (non-LTS). Use [Java support](JAVA-SUPPORT.md) for current requirements and test commands. The text below describes the earlier 0.2.0 baseline and is retained only as history; it is not the current support policy.

Version 0.2.0 requires JDK 27 to build and Java 27 to run. It uses stable Java APIs only; neither the library nor its tests require `--enable-preview`. This is a runtime compatibility change from 0.1.0, which targeted Java 17. The SQL migration, persisted states, token/generation protocol, and public method signatures are unchanged.

## Features used deliberately

| Stable capability | Use in this library | Reason |
|---|---|---|
| Sequenced collections, available since Java 21 | `List.getFirst()` in `PgTaskQueue.find` and single-lease test helpers | Expresses first-element access directly. The empty-result guard remains in place, preserving an empty `Mono` for unknown tasks. |
| Virtual threads and an auto-closeable executor, available since Java 21 | A PostgreSQL integration test starts 12 independent worker clients with `Executors.newVirtualThreadPerTaskExecutor()` | Checks that imperative callers on virtual threads can concurrently claim, heartbeat, and complete tasks through the existing reactive API. Executor lifetime is scoped to the test. |
| Records and text blocks, retained from the earlier implementation | `TaskRequest`, `TaskLease`, `TaskSnapshot`, and readable parameterized SQL | Keep immutable task snapshots and SQL review straightforward. These are existing design choices, not new Java 27 features. |

These features were introduced before Java 27 and are stable in the selected baseline. The project does not claim that moving the version number alone improves queue performance.

## Runtime implications

An application using imperative handlers can run waiting tasks on virtual threads instead of dedicating one platform thread to every waiting handler. That option is useful at an application boundary; R2DBC already performs database I/O asynchronously. The library therefore does not add an executor, a scheduler hop, or a blocking wrapper to each operation.

The Java 27 runtime includes the newer virtual-thread implementation, including support for unmounting during monitor-blocked operations introduced in JDK 24. This reduces one historical source of carrier-thread pinning in application code; it is not a guarantee that every native operation or dependency is free of pinning. Neither this behavior nor GC/JIT changes replace workload-specific measurement. There are no latency, throughput, allocation, or scale claims for this upgrade.

Virtual threads also do not increase PostgreSQL connection capacity. Bound concurrent handlers and size the connection pool deliberately. Do not call `.block()` from Reactor event-loop callbacks. In the concurrency test, blocking occurs only in the independent virtual-thread caller; the queue implementation stays asynchronous.

## Context and lifetime

Thread-local or scoped values are not used to carry lease ownership through a reactive chain. A subscription can execute callbacks on different threads, so thread-bound context is not an implicit substitute for Reactor `Context`. The task token and generation remain explicit values in `TaskLease`; SQL remains the ownership authority.

Preview language features and preview concurrency APIs are unnecessary for this implementation. Introducing them would add build/runtime flags and compatibility constraints without improving the current lease protocol. Structured task lifetime in the test is handled by the stable executor API and bounded waits.

## Verification

`virtualThreadWorkersCompleteEveryTaskWithoutDuplicateOwnership` inserts 96 tasks, starts 12 virtual-thread workers behind a shared start gate, and verifies:

- Each worker actually runs on a virtual thread.
- Every submitted task is claimed exactly once in this no-failure scenario.
- Claim results have the expected owner, first attempt, and first generation.
- Heartbeat and completion succeed for every acquired lease.
- All 96 database rows end in `DONE` with one attempt and the queue is empty afterward.

This test supplements the existing reactive concurrency, expiry, stale capability, retry, and lock-wait tests against real PostgreSQL. It does not simulate process crashes or prove exactly-once external effects. The existing failure-boundary documentation still applies.

## Dependency diagnostics

The tested R2DBC PostgreSQL 1.1.3.RELEASE dependency graph includes Reactor Netty 1.1.22 and Netty 4.1.112.Final. On Oracle JDK 27.0.1.0.2, these transitive dependencies emit native-access and `sun.misc.Unsafe` warnings. The integration suite passes without suppressing those warnings or adding native-access flags. This does not imply that the same dependency versions will remain compatible with future JDK removals. The library itself depends only on Reactor Core and R2DBC SPI; the application owns its driver and network dependency policy.

## References

- Oracle: [Virtual threads](https://docs.oracle.com/en/java/javase/25/core/virtual-threads.html), including executor lifetime and pinning behavior.
- Java API: [SequencedCollection](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/SequencedCollection.html), stable since Java 21.
- OpenJDK: [Synchronize virtual threads without pinning, JEP 491](https://openjdk.org/jeps/491).

[← Back to documentation and versions / Назад к документации и версиям](index.md)
