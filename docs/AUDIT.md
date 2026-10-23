# OSS-007 — queue and documentation audit

[README](../README.md) · [Versions](index.md) · [English guide](0.3.0/en.md) · [Русское руководство](0.3.0/ru.md)

This audit covers version 0.3.0: atomic claims, token/generation fencing, heartbeat, retries, expiry recovery, parameter binding, connection release, documentation examples and artifact consumption. Production SQL, public methods and schema remain unchanged: the review and added tests did not demonstrate a defect in those mechanisms.

## Findings and fixes

| Finding | Fix and evidence |
|---|---|
| Both guides contained unescaped JSON in Java string literals. Copying those examples failed compilation. | Corrected the strings. DocumentationTest compiles all seven operation snippets against the common setup with `--release 17` and checks identical EN/RU Java snippets. |
| Integration setup implicitly selected localhost and dropped an existing task table in any database ending in `_test`. A mistaken URL could destroy retained tasks. | Require an explicit URL and a fresh table. Removed the DROP. An intentional run against a populated fixture failed with “already exists”; all 199 original rows remained. |
| The default Russian README had no reproducible quick start. | Added build/test and local-artifact instructions in both languages while preserving one README with SVG EN/RU details controls. |
| The documented container commands used a fixed name/port, had an unlimited readiness loop and left cleanup until the happy-path final command. | Use the exact newly returned container ID, a dynamic loopback port, a finite readiness loop and exit/signal cleanup. Added `scripts/test-isolated.sh`; every invocation forces its tests to run against a fresh fixture. |
| Guide examples and the downloadable migration could silently drift. | Compile real guide snippets in the test suite and compare the downloadable migration to the packaged resource. Check local paths and GitHub anchors with `python3 scripts/check-docs.py`; this check was run locally. |

No guide website assets or Pages deployment were present in this repository. Documentation remains Markdown; the runnable Java consumer is preserved.

## Verified ownership and resource boundaries

- Existing tests cover disjoint parallel claims, 12 independent workers completing 96 tasks, queue/priority/delay behavior, idempotent submission, exhausted retries, stale capabilities and expiry while acknowledgment waits on a lock.
- A new lock-controlled test proves a short heartbeat preserves an uncommitted concurrent one-hour renewal once the lock is released.
- Another starts completion with an old capability while a replacement token/generation is uncommitted, observes the database lock wait, and verifies the old acknowledgment is rejected after commit.
- Six concurrent recovery passes recover 24 expired tasks exactly once in total, preserve the claim count and apply the configured retry delay.
- SQL-shaped queue names, payloads, owners and diagnostics round-trip as bound values without changing the schema.
- Six SPI lifecycle tests verify cold subscriptions, release before result emission, cleanup after binding/database failure, cleanup on cancellation, rejection/release of non-auto-commit connections, and release-error propagation. These tests exercise Reactor resource signals; they do not simulate a network partition.

The SQL lock reasoning was checked against PostgreSQL's [Read Committed behavior](https://www.postgresql.org/docs/17/transaction-iso.html#XACT-READ-COMMITTED) and [locking clauses](https://www.postgresql.org/docs/17/sql-select.html#SQL-FOR-UPDATE-SHARE). The real-database interleaving tests verify the library's specific statements.

## Actual verification

| Check | Result |
|---|---|
| Java 17 / 21 / 25 / 27 build and tests | 28 tests passed on each runtime; zero failures/skips. 18 PostgreSQL integration, 6 resource-lifecycle, 2 documentation and 2 validation tests. |
| PostgreSQL | Real PostgreSQL 17.6 in a disposable local process, separate databases per runtime. |
| Runtime worker mode | Platform workers on Java 17, actual virtual workers on 21/25/27. |
| Java API/bytecode baseline | `--release 17`; class-file major 61, no preview marker; Gradle metadata JVM 17. |
| Clean consumer | Artifact installed into a fresh repo-local Maven repository; copied Example.java/pom.xml/schema.sql compiled using Java 17 and executed against a fresh database. Expected final output: `finished: 2 DONE, 1 DEAD; no ready tasks`. |
| Markdown navigation | README → version index → EN/RU guides → README/back links, heading anchors and SVG paths validated. |
| Existing-table refusal | Expected setup failure; 199 retained rows unchanged. |

Docker's existing image store had missing snapshots, so no running containers were changed. The actual database checks used checksum-verified PostgreSQL binaries in an ignored repository-local fixture. The Docker wrapper and guide shell snippets passed shell syntax validation; their container startup path was not executed successfully in this environment. CI's fresh PostgreSQL service remains the Docker execution path to verify remotely.

Java 25/27 emit upstream Netty native-access warnings. Tests pass without preview flags or suppressing these warnings; this audit does not upgrade the selected PostgreSQL driver's transitive dependencies. Remote CI, Maven Central publication and production rollout were not performed.

## Operational limits

At-least-once attempts remain bounded by `maxAttempts` and depend on active workers/recovery and PostgreSQL durability. The token and generation fence queue rows, not external effects. A failed response or cancellation can follow a committed statement; do not infer rollback or generate replacement ownership. Cleanup of DONE/DEAD rows is application policy and removes UUID submission deduplication history.

Connection-pool acquisition, statement/lock timeouts, polling/backoff, heartbeats and recovery scheduling belong to the application. `SKIP LOCKED` avoids row-lock contention for claims/recovery, but it does not guarantee that every database wait is bounded. Configure database/driver budgets and caller cancellation for your deployment. No throughput, fairness, network-fault, HA or cross-region guarantees were added.
