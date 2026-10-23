package io.github.alex59.pgtasklease;

import static org.junit.jupiter.api.Assertions.*;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Timeout(20)
class PgTaskQueueIntegrationTest {
    private static final Duration WAIT = Duration.ofSeconds(10);
    private static ConnectionFactory connections;
    private static PgTaskQueue client;
    private String queue;

    @BeforeAll static void schema() throws Exception {
        System.out.println("Integration runtime: " + System.getProperty("java.runtime.version")
                + "; worker mode: " + (Runtime.version().feature() >= 21 ? "virtual" : "platform"));
        String url = System.getenv().getOrDefault("PG_TASK_TEST_URL",
                "r2dbc:postgresql://localhost:5432/pg_task_lease_test");
        var options = ConnectionFactoryOptions.parse(url);
        String database = (String) options.getRequiredValue(ConnectionFactoryOptions.DATABASE);
        if (!database.endsWith("_test")) {
            throw new IllegalArgumentException("Integration suite requires a disposable database ending in _test");
        }
        connections = ConnectionFactories.get(options.mutate()
                .option(ConnectionFactoryOptions.USER, System.getenv().getOrDefault("PG_TASK_TEST_USER", "lease_test"))
                .option(ConnectionFactoryOptions.PASSWORD, System.getenv().getOrDefault("PG_TASK_TEST_PASSWORD", "lease_test_only"))
                .build());
        client = new PgTaskQueue(connections);
        execute("DROP TABLE IF EXISTS public.pg_task_lease").block(WAIT);
        try (var schema = PgTaskQueue.class.getResourceAsStream("/pg-task-lease.sql")) {
            assertNotNull(schema);
            for (String sql : new String(schema.readAllBytes(), StandardCharsets.UTF_8).split(";")) {
                if (!sql.isBlank()) execute(sql).block(WAIT);
            }
        }
    }

    @BeforeEach void separateQueue() { queue = "test-" + UUID.randomUUID(); }

    @Test void deduplicatesSubmissionsWithoutChangingOriginal() {
        var request = request(2, 3);
        assertTrue(client.submit(request).block(WAIT));
        assertFalse(client.submit(new TaskRequest(request.id(), queue, "changed", 99, 1, Duration.ZERO)).block(WAIT));
        var lease = claim();
        assertEquals(request.payload(), lease.payload());
        assertEquals(2, lease.priority());
        assertEquals(3, lease.maxAttempts());
    }

    @Test void concurrentWorkersAcquireDisjointBatches() {
        int count = 60;
        Flux.range(0, count).concatMap(i -> client.submit(request(i % 3, 2))).then().block(WAIT);
        List<TaskLease> all = Flux.range(0, 6)
                .flatMap(worker -> client.claim(queue, "worker-" + worker, 10, Duration.ofSeconds(30)), 6)
                .flatMapIterable(batch -> batch).collectList().block(WAIT);
        assertEquals(count, all.size());
        assertEquals(count, new HashSet<>(all.stream().map(TaskLease::id).toList()).size());
        assertTrue(all.stream().allMatch(lease -> lease.attempt() == 1 && lease.generation() == 1));
        assertTrue(client.claim(queue, "extra", 10, Duration.ofSeconds(30)).block(WAIT).isEmpty());
    }

    @Test void independentWorkersCompleteEveryTaskWithoutDuplicateOwnership() throws Exception {
        int workerCount = 12;
        int count = 96;
        boolean virtualThreads = Runtime.version().feature() >= 21;
        var requests = java.util.stream.IntStream.range(0, count)
                .mapToObj(i -> request(i % 3, 2)).toList();
        Flux.fromIterable(requests).concatMap(client::submit).then().block(WAIT);
        var ready = new CountDownLatch(workerCount);
        var start = new CountDownLatch(1);
        var results = new ArrayList<Future<List<UUID>>>();
        var completed = new ArrayList<UUID>();
        ExecutorService workers = workerExecutor(workerCount, virtualThreads);
        try {
            try {
                for (int i = 0; i < workerCount; i++) {
                    String owner = (virtualThreads ? "virtual-worker-" : "platform-worker-") + i;
                    results.add(workers.submit(() -> {
                        assertEquals(virtualThreads, currentThreadIsVirtual());
                        var worker = new PgTaskQueue(connections);
                        var owned = new ArrayList<UUID>();
                        ready.countDown();
                        assertTrue(start.await(5, TimeUnit.SECONDS));
                        while (true) {
                            // Blocking belongs only to this imperative test caller, never the R2DBC implementation.
                            var batch = worker.claim(queue, owner, 4, Duration.ofSeconds(30)).block(WAIT);
                            if (batch.isEmpty()) return List.copyOf(owned);
                            for (TaskLease lease : batch) {
                                assertEquals(owner, lease.owner());
                                assertEquals(1, lease.attempt());
                                assertEquals(1L, lease.generation());
                                assertTrue(worker.heartbeat(lease, Duration.ofSeconds(30)).block(WAIT));
                                assertTrue(worker.complete(lease).block(WAIT));
                                owned.add(lease.id());
                            }
                        }
                    }));
                }
                assertTrue(ready.await(5, TimeUnit.SECONDS));
            } finally {
                start.countDown();
            }
            for (var result : results) completed.addAll(result.get(15, TimeUnit.SECONDS));
        } finally {
            // ExecutorService is not AutoCloseable on Java 17. Stop test-owned workers even on failure.
            workers.shutdownNow();
            assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS), "Workers did not terminate");
        }
        assertEquals(count, completed.size());
        assertEquals(count, new HashSet<>(completed).size());
        assertEquals(new HashSet<>(requests.stream().map(TaskRequest::id).toList()), new HashSet<>(completed));
        var snapshots = Flux.fromIterable(completed).flatMap(client::find, 8).collectList().block(WAIT);
        assertEquals(count, snapshots.size());
        assertTrue(snapshots.stream().allMatch(task -> task.state() == TaskState.DONE && task.attempts() == 1));
        assertTrue(client.claim(queue, "after-workers", 1, Duration.ofSeconds(30)).block(WAIT).isEmpty());
    }

    @Test void priorityAndQueueIsolation() {
        var low = request(-5, 2);
        var high = request(20, 2);
        var middle = request(0, 2);
        for (var task : List.of(low, high, middle)) client.submit(task).block(WAIT);
        client.submit(new TaskRequest(UUID.randomUUID(), "other-" + queue, "ignored", 100, 2, Duration.ZERO)).block(WAIT);
        var leases = client.claim(queue, "worker", 10, Duration.ofSeconds(30)).block(WAIT);
        assertEquals(List.of(high.id(), middle.id(), low.id()), leases.stream().map(TaskLease::id).toList());
        assertThrows(UnsupportedOperationException.class, () -> leases.clear());
    }

    @Test void honorsDelayAndEmptyQueue() {
        assertTrue(client.claim(queue, "worker", 1, Duration.ofSeconds(30)).block(WAIT).isEmpty());
        client.submit(new TaskRequest(UUID.randomUUID(), queue, "later", 0, 1, Duration.ofHours(1))).block(WAIT);
        assertTrue(client.claim(queue, "worker", 1, Duration.ofSeconds(30)).block(WAIT).isEmpty());
    }

    @Test void heartbeatExtendsButNeverShortensAndCompletionIsFenced() {
        client.submit(request(0, 2)).block(WAIT);
        TaskLease lease = claim();
        assertTrue(client.heartbeat(lease, Duration.ofMinutes(2)).block(WAIT));
        var expiry = client.find(lease.id()).block(WAIT).expiresAt();
        assertTrue(expiry.isAfter(lease.expiresAt()));
        assertTrue(client.heartbeat(lease, Duration.ofMillis(1)).block(WAIT));
        assertEquals(expiry, client.find(lease.id()).block(WAIT).expiresAt());
        assertTrue(client.complete(lease).block(WAIT));
        assertFalse(client.complete(lease).block(WAIT));
        assertFalse(client.heartbeat(lease, Duration.ofSeconds(10)).block(WAIT));
        assertEquals(TaskState.DONE, client.find(lease.id()).block(WAIT).state());
    }

    @Test void expiredWorkerCannotCompleteHeartbeatOrFailBeforeRecovery() {
        client.submit(request(0, 2)).block(WAIT);
        TaskLease lease = claim();
        expire(lease);
        assertFalse(client.complete(lease).block(WAIT));
        assertFalse(client.heartbeat(lease, Duration.ofSeconds(30)).block(WAIT));
        assertFalse(client.fail(lease, Duration.ZERO, "too late").block(WAIT));
    }

    @Test void recoveryChangesCapabilityAndRejectsStaleGenerationOrToken() {
        client.submit(request(0, 3)).block(WAIT);
        TaskLease old = claim();
        expire(old);
        assertEquals(1, client.recoverExpired(queue, 10, Duration.ZERO).block(WAIT));
        TaskLease next = claim();
        assertEquals(old.id(), next.id());
        assertEquals(old.generation() + 1, next.generation());
        assertNotEquals(old.token(), next.token());
        assertEquals(2, next.attempt());
        assertFalse(client.complete(old).block(WAIT));
        TaskLease wrongGeneration = new TaskLease(next.id(), next.queue(), next.payload(), next.priority(),
                next.attempt(), next.maxAttempts(), next.token(), old.generation(), next.owner(), next.expiresAt());
        assertFalse(client.complete(wrongGeneration).block(WAIT));
        assertTrue(client.complete(next).block(WAIT));
    }

    @Test void failureRetriesThenExhaustsAttemptBudget() {
        client.submit(request(0, 2)).block(WAIT);
        TaskLease first = claim();
        assertTrue(client.fail(first, Duration.ZERO, "temporary").block(WAIT));
        TaskLease second = claim();
        assertEquals(2, second.attempt());
        assertFalse(client.fail(first, Duration.ZERO, "old").block(WAIT));
        assertTrue(client.fail(second, Duration.ZERO, "final").block(WAIT));
        var snapshot = client.find(first.id()).block(WAIT);
        assertEquals(TaskState.DEAD, snapshot.state());
        assertEquals("final", snapshot.lastError());
        assertTrue(client.claim(queue, "worker", 1, Duration.ofSeconds(30)).block(WAIT).isEmpty());
    }

    @Test void failedTasksRespectRetryDelay() {
        client.submit(request(0, 2)).block(WAIT);
        TaskLease lease = claim();
        assertTrue(client.fail(lease, Duration.ofHours(1), "retry later").block(WAIT));
        assertEquals(TaskState.READY, client.find(lease.id()).block(WAIT).state());
        assertTrue(client.claim(queue, "worker", 1, Duration.ofSeconds(30)).block(WAIT).isEmpty());
    }

    @Test void recoveryHonorsBudgetBatchLimitAndLiveLeases() {
        for (int i = 0; i < 3; i++) client.submit(request(0, 1)).block(WAIT);
        var leases = client.claim(queue, "worker", 3, Duration.ofSeconds(30)).block(WAIT);
        expire(leases.get(0));
        expire(leases.get(1));
        assertEquals(1, client.recoverExpired(queue, 1, Duration.ZERO).block(WAIT));
        assertEquals(1, client.recoverExpired(queue, 10, Duration.ZERO).block(WAIT));
        assertEquals(0, client.recoverExpired(queue, 10, Duration.ZERO).block(WAIT));
        assertEquals(TaskState.DEAD, client.find(leases.get(0).id()).block(WAIT).state());
        assertEquals(TaskState.LEASED, client.find(leases.get(2).id()).block(WAIT).state());
    }

    @Test void waitingForRowLockDoesNotAuthorizeExpiredAcknowledgement() {
        client.submit(request(0, 2)).block(WAIT);
        TaskLease lease = client.claim(queue, "worker", 1, Duration.ofMillis(300)).block(WAIT).get(0);
        Connection blocker = Mono.from(connections.create()).block(WAIT);
        try {
            Mono.from(blocker.beginTransaction()).block(WAIT);
            Flux.from(blocker.createStatement("SELECT id FROM public.pg_task_lease WHERE id = $1 FOR UPDATE")
                    .bind(0, lease.id()).execute()).flatMap(r -> r.map((row, md) -> row.get(0))).then().block(WAIT);
            CompletableFuture<Boolean> result = client.complete(lease).toFuture();
            // Keep the row locked until the lease expires; the waiting command started while valid.
            Flux.from(blocker.createStatement("SELECT pg_sleep(0.5)").execute())
                    .flatMap(r -> r.map((row, md) -> 1)).then().block(WAIT);
            Mono.from(blocker.commitTransaction()).block(WAIT);
            assertFalse(result.join());
        } finally {
            Mono.from(blocker.close()).block(WAIT);
        }
    }

    @Test void findMissingIsEmpty() { assertNull(client.find(UUID.randomUUID()).block(WAIT)); }

    @Test void readmeBatchFlowAcknowledgesHandlerCompletion() {
        var request = request(10, 5);
        var handled = new HashSet<UUID>();
        Mono<Boolean> submitted = client.submit(request);
        Mono<Void> oneBatch = submitted.then(
                client.claim(queue, "worker-1", 4, Duration.ofSeconds(30))
                        .flatMapMany(Flux::fromIterable)
                        .flatMap(lease -> Mono.fromRunnable(() -> handled.add(lease.id()))
                                .then(client.complete(lease))
                                .flatMap(acknowledged -> acknowledged ? Mono.<Void>empty()
                                        : Mono.error(new IllegalStateException("Lease was lost"))), 4)
                        .then());
        oneBatch.block(WAIT);
        assertEquals(java.util.Set.of(request.id()), handled);
        assertEquals(TaskState.DONE, client.find(request.id()).block(WAIT).state());
    }

    private TaskRequest request(int priority, int attempts) {
        return new TaskRequest(UUID.randomUUID(), queue, "{\"kind\":\"email\"}", priority, attempts, Duration.ZERO);
    }

    private TaskLease claim() {
        return client.claim(queue, "worker", 1, Duration.ofSeconds(30)).block(WAIT).get(0);
    }

    private static ExecutorService workerExecutor(int count, boolean virtualThreads)
            throws ReflectiveOperationException {
        if (!virtualThreads) return Executors.newFixedThreadPool(count);
        // Test-only bridge: retain --release 17 while exercising real virtual threads on 21/25.
        // Reflection failure must fail the test instead of silently switching to platform threads.
        return (ExecutorService) Executors.class.getMethod("newVirtualThreadPerTaskExecutor").invoke(null);
    }

    private static boolean currentThreadIsVirtual() throws ReflectiveOperationException {
        if (Runtime.version().feature() < 21) return false;
        return (boolean) Thread.class.getMethod("isVirtual").invoke(Thread.currentThread());
    }

    private void expire(TaskLease lease) {
        execute("UPDATE public.pg_task_lease SET lease_until = clock_timestamp() - interval '1 second' WHERE id = '"
                + lease.id() + "'").block(WAIT);
    }

    private static Mono<Void> execute(String sql) {
        return Mono.usingWhen(Mono.from(connections.create()), c -> Flux.from(c.createStatement(sql).execute())
                .concatMap(result -> result.getRowsUpdated()).then(), c -> Mono.from(c.close()));
    }
}
