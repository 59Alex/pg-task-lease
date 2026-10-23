package io.github.alex59.pgtasklease;

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Non-blocking PostgreSQL task leases. Each subscription uses and closes one auto-commit connection.
 * No polling threads, handler invocation, or recovery scheduler are installed implicitly.
 */
public final class PgTaskQueue {
    private final ConnectionFactory connections;

    /**
     * Creates a queue client. The factory may be a caller-managed R2DBC pool.
     * @param connections factory supplying auto-commit connections
     */
    public PgTaskQueue(ConnectionFactory connections) {
        this.connections = Objects.requireNonNull(connections, "connections");
    }

    /**
     * Inserts a task; returns false when the id already exists, without modifying the stored task.
     * The id is global across queues. A repeated subscription repeats this idempotent submission.
     * @param task validated request
     * @return whether a new row was inserted
     */
    public Mono<Boolean> submit(TaskRequest task) {
        Objects.requireNonNull(task, "task");
        return changed("""
                INSERT INTO public.pg_task_lease (id, queue, payload, priority, max_attempts, available_at)
                VALUES ($1, $2, $3, $4, $5, clock_timestamp() + $6 * interval '1 millisecond')
                ON CONFLICT (id) DO NOTHING RETURNING id
                """, s -> s.bind(0, task.id()).bind(1, task.queue()).bind(2, task.payload())
                .bind(3, task.priority()).bind(4, task.maxAttempts()).bind(5, task.delay().toMillis()));
    }

    /**
     * Atomically claims up to limit ready tasks, higher priority first, then creation time and id.
     * The complete batch is consumed before it is emitted. SKIP LOCKED can bypass a locked higher
     * priority task, so priority is a preference, not a global execution order.
     *
     * @param queue queue name
     * @param owner diagnostic worker name; ownership is enforced by token and generation
     * @param limit maximum batch size, from 1 to 1000
     * @param leaseTime lifetime from database wall clock; at least one millisecond
     * @return immutable batch, empty if no task is eligible
     */
    public Mono<List<TaskLease>> claim(String queue, String owner, int limit, Duration leaseTime) {
        Checks.text(queue, "queue", 120);
        Checks.text(owner, "owner", 200);
        batchSize(limit);
        long millis = Checks.millis(leaseTime, "leaseTime", true);
        return query("""
                WITH candidates AS MATERIALIZED (
                    SELECT id FROM public.pg_task_lease
                    WHERE queue = $1 AND state = 'READY' AND available_at <= clock_timestamp()
                        AND attempts < max_attempts
                    ORDER BY priority DESC, created_at, id
                    LIMIT $2 FOR UPDATE SKIP LOCKED
                ), claimed AS (
                    UPDATE public.pg_task_lease t
                    SET state = 'LEASED', owner = $3, lease_token = gen_random_uuid(),
                        generation = generation + 1, attempts = attempts + 1,
                        lease_until = clock_timestamp() + $4 * interval '1 millisecond',
                        updated_at = clock_timestamp()
                    FROM candidates c WHERE t.id = c.id
                    RETURNING t.*
                ) SELECT * FROM claimed ORDER BY priority DESC, created_at, id
                """, s -> s.bind(0, queue).bind(1, limit).bind(2, owner).bind(3, millis),
                row -> new TaskLease(row.get("id", UUID.class), row.get("queue", String.class),
                        row.get("payload", String.class), row.get("priority", Integer.class),
                        row.get("attempts", Integer.class), row.get("max_attempts", Integer.class),
                        row.get("lease_token", UUID.class), row.get("generation", Long.class),
                        row.get("owner", String.class), instant(row, "lease_until")));
    }

    /**
     * Extends an unexpired current lease to at least database-now plus leaseTime.
     * Returns false for a lost lease. It never revives an expired lease or shortens its expiry.
     * The supplied TaskLease remains an immutable acquisition snapshot.
     * @param lease acquired capability
     * @param leaseTime minimum remaining duration from database-now
     * @return whether the current lease was extended or preserved
     */
    public Mono<Boolean> heartbeat(TaskLease lease, Duration leaseTime) {
        long millis = Checks.millis(leaseTime, "leaseTime", true);
        return leasedUpdate(lease, """
                lease_until = GREATEST(t.lease_until, clock_timestamp() + $4 * interval '1 millisecond'),
                updated_at = clock_timestamp()
                """, s -> s.bind(3, millis));
    }

    /**
     * Marks a current unexpired lease DONE. Returns false if ownership was lost or already completed.
     * @param lease acquired capability
     * @return whether this call changed the task to DONE
     */
    public Mono<Boolean> complete(TaskLease lease) {
        return leasedUpdate(lease, """
                state = 'DONE', lease_token = NULL, owner = NULL, lease_until = NULL,
                updated_at = clock_timestamp()
                """, s -> { });
    }

    /**
     * Releases a current lease for a delayed retry, or marks it DEAD when attempts are exhausted.
     * Diagnostic error text is stored verbatim; callers must remove sensitive values.
     * @param lease acquired capability
     * @param retryDelay delay before the next attempt
     * @param error failure description, at most 2048 characters
     * @return whether the failure transition was applied
     */
    public Mono<Boolean> fail(TaskLease lease, Duration retryDelay, String error) {
        long millis = Checks.millis(retryDelay, "retryDelay", false);
        Objects.requireNonNull(error, "error");
        if (error.length() > 2048) throw new IllegalArgumentException("error must be at most 2048 characters");
        return leasedUpdate(lease, """
                state = CASE WHEN t.attempts >= t.max_attempts THEN 'DEAD' ELSE 'READY' END,
                available_at = clock_timestamp() + $4 * interval '1 millisecond',
                last_error = $5, lease_token = NULL, owner = NULL, lease_until = NULL,
                updated_at = clock_timestamp()
                """, s -> s.bind(3, millis).bind(4, error));
    }

    /**
     * Requeues up to limit expired leases, or marks exhausted tasks DEAD. Run periodically.
     * Recovery uses SKIP LOCKED and can safely run in several worker processes.
     * @param queue logical queue name
     * @param limit maximum recovered batch, 1..1000
     * @param retryDelay delay before reclaimed tasks become eligible
     * @return number of recovered rows
     */
    public Mono<Integer> recoverExpired(String queue, int limit, Duration retryDelay) {
        Checks.text(queue, "queue", 120);
        batchSize(limit);
        long millis = Checks.millis(retryDelay, "retryDelay", false);
        return query("""
                WITH expired AS MATERIALIZED (
                    SELECT id FROM public.pg_task_lease
                    WHERE queue = $1 AND state = 'LEASED' AND lease_until <= clock_timestamp()
                    ORDER BY lease_until, id LIMIT $2 FOR UPDATE SKIP LOCKED
                )
                UPDATE public.pg_task_lease t
                SET state = CASE WHEN t.attempts >= t.max_attempts THEN 'DEAD' ELSE 'READY' END,
                    available_at = clock_timestamp() + $3 * interval '1 millisecond',
                    last_error = 'Lease expired', lease_token = NULL, owner = NULL, lease_until = NULL,
                    updated_at = clock_timestamp()
                FROM expired e WHERE t.id = e.id RETURNING t.id
                """, s -> s.bind(0, queue).bind(1, limit).bind(2, millis),
                row -> row.get("id", UUID.class)).map(List::size);
    }

    /**
     * Returns a diagnostic snapshot, or an empty Mono if the id is unknown.
     * @param id task id
     * @return current snapshot, empty for an unknown id
     */
    public Mono<TaskSnapshot> find(UUID id) {
        Objects.requireNonNull(id, "id");
        return query("SELECT * FROM public.pg_task_lease WHERE id = $1", s -> s.bind(0, id),
                row -> new TaskSnapshot(row.get("id", UUID.class), row.get("queue", String.class),
                        TaskState.valueOf(row.get("state", String.class)), row.get("attempts", Integer.class),
                        row.get("max_attempts", Integer.class), row.get("generation", Long.class),
                        instant(row, "available_at"), instant(row, "lease_until"),
                        row.get("owner", String.class), row.get("last_error", String.class)))
                .flatMap(rows -> rows.isEmpty() ? Mono.empty() : Mono.just(rows.get(0)));
    }

    private Mono<Boolean> leasedUpdate(TaskLease lease, String assignments, Consumer<Statement> bindExtra) {
        Objects.requireNonNull(lease, "lease");
        Objects.requireNonNull(lease.id(), "lease.id");
        Objects.requireNonNull(lease.token(), "lease.token");
        // Lock first, then check wall-clock expiry: a wait on a row lock must not let a stale
        // pre-lock timestamp authorize an acknowledgement after the lease has expired.
        String sql = """
                WITH owned AS MATERIALIZED (
                    SELECT * FROM public.pg_task_lease WHERE id = $1 FOR UPDATE
                )
                UPDATE public.pg_task_lease t SET
                """ + assignments + """
                FROM owned o WHERE t.id = o.id AND o.state = 'LEASED'
                    AND o.lease_token = $2 AND o.generation = $3 AND o.lease_until > clock_timestamp()
                RETURNING t.id
                """;
        return changed(sql, s -> {
            s.bind(0, lease.id()).bind(1, lease.token()).bind(2, lease.generation());
            bindExtra.accept(s);
        });
    }

    private Mono<Boolean> changed(String sql, Consumer<Statement> bind) {
        return query(sql, bind, row -> row.get("id", UUID.class)).map(rows -> !rows.isEmpty());
    }

    private <T> Mono<List<T>> query(String sql, Consumer<Statement> bind, Function<Row, T> map) {
        return Mono.usingWhen(Mono.from(connections.create()), connection -> {
            if (!connection.isAutoCommit()) {
                return Mono.error(new IllegalStateException("ConnectionFactory must supply auto-commit connections"));
            }
            Statement statement = connection.createStatement(sql);
            bind.accept(statement);
            return Flux.from(statement.execute())
                    .concatMap(result -> result.map((row, metadata) -> map.apply(row)))
                    .collectList().map(List::copyOf);
        }, connection -> Mono.from(connection.close()),
                (connection, error) -> Mono.from(connection.close()),
                connection -> Mono.from(connection.close()));
    }

    private static Instant instant(Row row, String name) {
        OffsetDateTime value = row.get(name, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }

    private static void batchSize(int limit) {
        if (limit < 1 || limit > 1000) throw new IllegalArgumentException("limit must be between 1 and 1000");
    }
}
