package io.github.alex59.pgtasklease;

import static org.junit.jupiter.api.Assertions.*;

import io.r2dbc.spi.*;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

class ResourceLifecycleTest {
    private static final Duration WAIT = Duration.ofSeconds(5);

    @Test void closesBeforeEmittingAndAcquiresAgainForEachSubscription() {
        var fixture = new Fixture();
        fixture.closeGate = Sinks.empty();
        var operation = fixture.queue.submit(request());
        assertEquals(0, fixture.created.get());
        var result = operation.toFuture();
        assertEquals(1, fixture.closed.get());
        assertFalse(result.isDone(), "The result must wait for connection release");
        fixture.closeGate.tryEmitEmpty();
        assertTrue(result.join());
        assertTrue(operation.block(WAIT));
        assertEquals(2, fixture.created.get());
        assertEquals(2, fixture.closed.get());
    }

    @Test void bindingFailureStillClosesTheAcquiredConnection() {
        var fixture = new Fixture();
        fixture.bindFailure = new IllegalArgumentException("bind rejected");
        assertSame(fixture.bindFailure, assertThrows(IllegalArgumentException.class,
                () -> fixture.queue.submit(request()).block(WAIT)));
        assertEquals(1, fixture.closed.get());
    }

    @Test void databaseFailureClosesConnectionAndPreservesFailure() {
        var fixture = new Fixture();
        fixture.execution = Mono.error(new IllegalStateException("database failure"));
        assertEquals("database failure", assertThrows(IllegalStateException.class,
                () -> fixture.queue.submit(request()).block(WAIT)).getMessage());
        assertEquals(1, fixture.closed.get());
    }

    @Test void cancellationClosesConnectionWithoutEmittingSuccess() {
        var fixture = new Fixture();
        fixture.execution = Mono.never();
        var subscription = fixture.queue.submit(request()).subscribe(value -> fail("Cancelled operation emitted"));
        assertEquals(1, fixture.created.get());
        subscription.dispose();
        assertEquals(1, fixture.closed.get());
    }

    @Test void transactionBoundConnectionIsRejectedAndReleasedBeforeExecutingSql() {
        var fixture = new Fixture();
        fixture.autoCommit = false;
        assertTrue(assertThrows(IllegalStateException.class,
                () -> fixture.queue.submit(request()).block(WAIT)).getMessage().contains("auto-commit"));
        assertEquals(0, fixture.statements.get());
        assertEquals(1, fixture.closed.get());
    }

    @Test void releaseFailureCannotBeReportedAsSuccessfulAcknowledgement() {
        var fixture = new Fixture();
        fixture.closeGate = Sinks.empty();
        fixture.closeGate.tryEmitError(new IllegalStateException("release failure after SQL result"));
        var error = assertThrows(RuntimeException.class, () -> fixture.queue.submit(request()).block(WAIT));
        assertNotNull(error.getCause());
        assertEquals("release failure after SQL result", error.getCause().getMessage());
        assertEquals(1, fixture.closed.get());
    }

    private static TaskRequest request() {
        return new TaskRequest(UUID.randomUUID(), "resource-test", "payload", 0, 1, Duration.ZERO);
    }

    /** Small SPI fake: tests lifecycle signals, not PostgreSQL SQL semantics. */
    private static final class Fixture {
        final AtomicInteger created = new AtomicInteger();
        final AtomicInteger closed = new AtomicInteger();
        final AtomicInteger statements = new AtomicInteger();
        boolean autoCommit = true;
        RuntimeException bindFailure;
        Sinks.Empty<Void> closeGate;
        Mono<Result> execution = Mono.just(proxy(Result.class, (method, args) -> {
            if (method.equals("map")) {
                @SuppressWarnings("unchecked")
                var mapper = (BiFunction<Row, RowMetadata, ?>) args[0];
                return Mono.just(mapper.apply(proxy(Row.class, (name, values) -> UUID.randomUUID()), null));
            }
            throw new UnsupportedOperationException(method);
        }));
        final PgTaskQueue queue = new PgTaskQueue(new ConnectionFactory() {
            @Override public Mono<? extends Connection> create() {
                return Mono.fromSupplier(() -> {
                    created.incrementAndGet();
                    return proxy(Connection.class, (method, args) -> switch (method) {
                        case "isAutoCommit" -> autoCommit;
                        case "createStatement" -> {
                            statements.incrementAndGet();
                            yield statement();
                        }
                        case "close" -> Mono.defer(() -> {
                            closed.incrementAndGet();
                            return closeGate == null ? Mono.empty() : closeGate.asMono();
                        });
                        default -> throw new UnsupportedOperationException(method);
                    });
                });
            }
            @Override public ConnectionFactoryMetadata getMetadata() { return () -> "test"; }
        });
        Statement statement() {
            final Statement[] statement = new Statement[1];
            statement[0] = proxy(Statement.class, (method, args) -> switch (method) {
                case "bind" -> {
                    if (bindFailure != null) throw bindFailure;
                    yield statement[0];
                }
                case "execute" -> execution;
                default -> throw new UnsupportedOperationException(method);
            });
            return statement[0];
        }
    }

    @FunctionalInterface private interface Call { Object invoke(String method, Object[] args); }
    private static <T> T proxy(Class<T> type, Call call) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type},
                (proxy, method, args) -> call.invoke(method.getName(), args)));
    }
}
