# Ownership before execution

[← Back to documentation and versions / Назад к документации и версиям](index.md)

`pg-task-lease` coordinates background work for independent application processes using a PostgreSQL table as the ownership authority. Its boundary is task state; business handlers, external systems, polling cadence, connection pools, and data retention belong to the application.

## Context and components

```text
Application producer -- submit --> PgTaskQueue -- R2DBC --> PostgreSQL
Application workers  -- claim / heartbeat / complete / fail --> PgTaskQueue
Recovery loop        -- recoverExpired --> PgTaskQueue
Application handler  -- idempotent effect --> External system
```

This is a component view of an embedded library, not a separate queue server. `PgTaskQueue` contains parameterized SQL and resource-lifetime handling. PostgreSQL owns durable state and all timing decisions. The connection factory/pool belongs to the host application. The external system is outside PostgreSQL's transaction boundary.

## State machine

```text
submit -> READY --claim--> LEASED --complete--> DONE
           ^                 |
           +-- fail/recover --+  (attempts < maxAttempts)
                             |
                             +-- fail/recover --> DEAD (budget exhausted)
```

An attempt is counted when claimed, even if a process crashes before invoking the handler. Terminal rows are retained. There is no implicit resurrection or retry after `DEAD`.

## Critical sequence

1. Producer chooses a stable UUID and submits. A unique primary key makes resubmission a no-op.
2. Worker selects eligible `READY` rows by queue and priority with `FOR UPDATE SKIP LOCKED`. In the same SQL statement it increments attempts and generation, generates fresh tokens, and stores database-clock expiry.
3. The client consumes the complete result and releases the auto-commit connection before emitting its batch. No transaction spans handler execution.
4. The handler performs its effect. For long work the application heartbeats before expiry.
5. Completion first locks the task row, then evaluates token, generation, state, and expiry using database wall clock. This ordering matters: an acknowledgement blocked on a row lock must not use a timestamp from before the wait.
6. A recovery process takes expired rows with `SKIP LOCKED`, clears ownership, and either schedules another attempt or marks the task `DEAD`.

## Invariants

- At most one stored capability (UUID token + monotonic generation) is current for a task.
- A stale or expired capability cannot complete, fail, or revive the task through this API.
- A claim and its attempt/generation increment are atomic; selection is not a separate client round trip.
- PostgreSQL time decides availability and expiry; worker wall clocks do not participate.
- Terminal state and `READY` have no token, owner, or expiry; a SQL constraint enforces this shape.
- The handler can still be running after losing ownership. These invariants protect **queue records**, not external side effects. Generation can be passed to an external fenced resource only if that resource implements its own comparison, with the appropriate task/resource scope.

## Failure boundaries

| Failure | Durable state and effect | Recovery / verification |
|---|---|---|
| Worker dies after claim | Task stays `LEASED`; handler may or may not have run. | Recovery requeues after expiry unless budget exhausted. Tested with forced expiry and re-claim. |
| Effect succeeds, process dies before ack | External effect exists; task remains leased. | Retry may duplicate it. Handler deduplicates by task id; this library cannot prove remote outcome. |
| `claim` response is lost or subscription cancelled | Some/all claims may already be committed, but caller may have no lease values. | Do not guess ownership. Let expiry recovery return tasks. |
| `complete` transport response is lost | Completion may already be committed. | Inspect state if useful; false from a repeated complete means no transition, not proof the first complete failed. |
| Pause exceeds lease, new worker claims | Old process may resume concurrently. | Old token/generation cannot change queue state; external idempotency is still needed. Tested. |
| Ack waits on a row lock past expiry | Task was valid before waiting and expired afterward. | Post-lock expiry check rejects it. Tested against real PostgreSQL. |
| Recovery is not scheduled | Expired tasks stay leased indefinitely. | Application must schedule and monitor recovery; database expiry alone does not execute code. |
| PostgreSQL is unavailable | No new ownership decisions can be confirmed. | Stop assuming lease ownership, back off, reconnect. HA replication/durability policy is an operator responsibility. |

## Trade-offs

| Decision | Benefit | Cost / when to reconsider |
|---|---|---|
| One table, atomic statements, no broker | Small operational surface; task state is inspectable SQL. | Queue traffic competes with application database traffic. Benchmark your load and use a broker if it exceeds the database budget. |
| Explicit leases and recovery | Crash recovery without process-local ownership. | Tune lease time and heartbeat cadence; monitor stale leases and retry rate. |
| Priority plus `SKIP LOCKED` | Workers avoid waiting behind each other's selected jobs. | Not strict FIFO; low priorities can starve. Add an application-level fairness policy when required. |
| Fixed `public.pg_task_lease` table | No dynamic SQL identifiers; predictable migration and permission surface. | No custom schema/table names in 0.3.0. Queue names isolate selection, not database access permissions. |
| Application-managed polling | No hidden threads or shutdown semantics. | The application must implement polling/backoff, heartbeat and recovery lifecycles. |
| Separate auto-commit operation | Ownership commits before handler work; short transactions. | Submission is not atomic with application domain writes. Use a transactional outbox if that boundary matters. |

The ready index favors queue and priority traversal. A large backlog of future tasks can increase filtering cost; measure plans and maintenance behavior with your workload. No throughput/SLA claim is implied by the integration suite.

Version 0.3.0 uses Java 17 as its minimum and supports the 17/21/25 LTS runtimes plus Java 27 (non-LTS) while retaining the same SQL schema and ownership protocol. [Java support](JAVA-SUPPORT.md) describes the compiler boundary and runtime-specific concurrency tests. The production API remains non-blocking R2DBC; virtual threads are an application-side execution choice and do not change task guarantees.

## What to know

The library grants temporary ownership and fences task-state transitions. PostgreSQL owns time and durability; the host owns scheduling, handling, pool limits, and external idempotency. Retries are bounded. At-least-once attempts require active workers and recovery, and never imply exactly-once effects.

Diagrams above are plain-text structural/state descriptions and require no renderer. References: PostgreSQL [locking clause](https://www.postgresql.org/docs/17/sql-select.html#SQL-FOR-UPDATE-SHARE), [date/time functions](https://www.postgresql.org/docs/17/functions-datetime.html), and the [PostgreSQL R2DBC driver](https://github.com/pgjdbc/r2dbc-postgresql).

[← Back to documentation and versions / Назад к документации и версиям](index.md)
