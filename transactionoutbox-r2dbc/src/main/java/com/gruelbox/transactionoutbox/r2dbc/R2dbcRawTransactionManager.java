package com.gruelbox.transactionoutbox.r2dbc;

import static io.r2dbc.spi.IsolationLevel.READ_COMMITTED;
import static reactor.core.publisher.Mono.fromCompletionStage;

import com.gruelbox.transactionoutbox.Beta;
import com.gruelbox.transactionoutbox.Invocation;
import com.gruelbox.transactionoutbox.TransactionalInvocation;
import com.gruelbox.transactionoutbox.spi.InitializationEventBus;
import com.gruelbox.transactionoutbox.spi.InitializationEventPublisher;
import com.gruelbox.transactionoutbox.spi.SerializableTypeRequired;
import com.gruelbox.transactionoutbox.spi.TransactionManagerSupport;
import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.ConnectionMetadata;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.ValidationDepth;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

/**
 * A transaction manager which uses the raw R2DBC {@link Connection} SPI as its context. This is
 * unlikely to be particularly useful in client code where a particular high-level R2DBC-compatible
 * API will be in use. It's provided mainly as a proof of concept.
 */
@Beta
@Slf4j
public class R2dbcRawTransactionManager
    implements R2dbcTransactionManager<R2dbcRawTransaction>, InitializationEventPublisher {

  private final ConnectionFactoryWrapper cf;

  private final ConcurrentMap<UUID, ConnectionLogger> openTransactions = new ConcurrentHashMap<>();
  private boolean stackLogging;

  public R2dbcRawTransactionManager(ConnectionFactoryWrapper cf) {
    this.cf = cf;
  }

  public static ConnectionFactoryWrapper wrapConnectionFactory(
      ConnectionFactory connectionFactory) {
    return new ConnectionFactoryWrapper(connectionFactory);
  }

  @Override
  public void onPublishInitializationEvents(InitializationEventBus eventBus) {
    eventBus.sendEvent(
        SerializableTypeRequired.class, new SerializableTypeRequired(R2dbcTransaction.class));
    eventBus.sendEvent(
        SerializableTypeRequired.class, new SerializableTypeRequired(R2dbcRawTransaction.class));
    eventBus.sendEvent(
        SerializableTypeRequired.class, new SerializableTypeRequired(Connection.class));
  }

  // For testing
  public Set<ConnectionLogger> getOpenTransactions() {
    return Set.copyOf(openTransactions.values());
  }

  // For testing
  public R2dbcRawTransactionManager enableStackLogging() {
    this.stackLogging = true;
    return this;
  }

  @Override
  public <T> CompletableFuture<T> transactionally(
      Function<R2dbcRawTransaction, CompletableFuture<T>> fn) {
    return withConnection(
            conn -> {
              var tx = cf.contextMap.get(conn);
              return begin(conn)
                  .then(fromCompletionStage(() -> fn.apply(tx).thenApply(Optional::ofNullable)))
                  .flatMap(result -> commit(conn, result))
                  .onErrorResume(t -> rollback(conn, t));
            })
        .toFuture()
        .thenApply(opt -> opt.orElse(null));
  }

  @Override
  public TransactionalInvocation extractTransaction(Method method, Object[] args) {
    return TransactionManagerSupport.extractTransactionFromInvocation(
        method, args, Connection.class, cf.contextMap::get);
  }

  @Override
  public Invocation injectTransaction(Invocation invocation, R2dbcRawTransaction transaction) {
    return TransactionManagerSupport.injectTransactionIntoInvocation(
        invocation, Connection.class, transaction);
  }

  private <T> Mono<T> begin(Connection conn) {
    return Mono.from(conn.beginTransaction()).then(Mono.empty());
  }

  private <T> Mono<T> commit(Connection conn, T result) {
    return Mono.from(conn.commitTransaction()).then(Mono.just(result));
  }

  private <T> Mono<T> rollback(Connection conn, Throwable e) {
    return Mono.fromRunnable(
            () ->
                log.warn(
                    "Exception in transactional block ({}{}). Rolling back. See later messages for detail",
                    e.getClass().getSimpleName(),
                    e.getMessage() == null ? "" : (" - " + e.getMessage())))
        .then(Mono.from(conn.rollbackTransaction()).then(Mono.empty()))
        .then(Mono.fromRunnable(() -> log.info("Rollback complete")))
        .then(Mono.error(e));
  }

  private <T> Mono<T> withConnection(Function<Connection, Mono<T>> fn) {
    return Mono.usingWhen(
        Mono.fromCallable(ConnectionLogger::new),
        logger ->
            Mono.usingWhen(
                Mono.from(cf.create()),
                conn -> setupConnection(conn).then(fn.apply(conn)),
                conn -> Mono.from(conn.close())),
        logger -> Mono.fromRunnable(logger::close));
  }

  private Mono<Void> setupConnection(Connection conn) {
    return (READ_COMMITTED.equals(conn.getTransactionIsolationLevel())
            ? Mono.empty()
            : Mono.from(conn.setTransactionIsolationLevel(READ_COMMITTED)))
        .then(conn.isAutoCommit() ? Mono.empty() : Mono.from(conn.setAutoCommit(false)));
  }

  @EqualsAndHashCode
  public final class ConnectionLogger implements AutoCloseable {
    UUID key = UUID.randomUUID();

    @EqualsAndHashCode.Exclude StackTraceElement[] stackTrace;

    ConnectionLogger() {
      if (stackLogging) {
        this.stackTrace = new Throwable().getStackTrace();
        openTransactions.put(key, this);
      } else {
        this.stackTrace = null;
      }
    }

    @Override
    public void close() {
      if (stackLogging) {
        openTransactions.remove(key);
      }
    }

    @Override
    public String toString() {
      return Arrays.stream(stackTrace)
          .map(StackTraceElement::toString)
          .collect(Collectors.joining("\n"));
    }
  }

  @AllArgsConstructor
  public static final class ConnectionFactoryWrapper implements ConnectionFactory {

    private final ConnectionFactory delegate;
    private final ConcurrentMap<Connection, R2dbcRawTransaction> contextMap =
        new ConcurrentHashMap<>();

    @Override
    public Publisher<? extends Connection> create() {
      return Mono.from(delegate.create())
          .map(WrappedConnection::new)
          .map(
              conn -> {
                log.trace("Adding transaction for connection {} to map", conn);
                contextMap.put(conn, new R2dbcRawTransaction(conn));
                return conn;
              });
    }

    @Override
    public ConnectionFactoryMetadata getMetadata() {
      return delegate.getMetadata();
    }

    private static final AtomicLong nextId = new AtomicLong(1);

    private class WrappedConnection implements Connection {

      private final Connection conn;
      private final long id = nextId.getAndIncrement();

      WrappedConnection(Connection conn) {
        this.conn = conn;
      }

      @Override
      public Publisher<Void> beginTransaction() {
        return conn.beginTransaction();
      }

      @Override
      public Publisher<Void> close() {
        return Mono.from(conn.close())
            .then(
                Mono.fromRunnable(
                    () -> {
                      log.trace("Removing transaction for connection {} from map", this);
                      contextMap.remove(this);
                    }));
      }

      @Override
      public Publisher<Void> commitTransaction() {
        return Mono.from(conn.commitTransaction())
            .then(fromCompletionStage(() -> contextMap.get(this).processHooks()));
      }

      @Override
      public Batch createBatch() {
        return conn.createBatch();
      }

      @Override
      public Publisher<Void> createSavepoint(String name) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Statement createStatement(String sql) {
        return conn.createStatement(sql);
      }

      @Override
      public boolean isAutoCommit() {
        return conn.isAutoCommit();
      }

      @Override
      public ConnectionMetadata getMetadata() {
        return conn.getMetadata();
      }

      @Override
      public IsolationLevel getTransactionIsolationLevel() {
        return conn.getTransactionIsolationLevel();
      }

      @Override
      public Publisher<Void> releaseSavepoint(String name) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Publisher<Void> rollbackTransaction() {
        return conn.rollbackTransaction();
      }

      @Override
      public Publisher<Void> rollbackTransactionToSavepoint(String name) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Publisher<Void> setAutoCommit(boolean autoCommit) {
        return conn.setAutoCommit(autoCommit);
      }

      @Override
      public Publisher<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
        return conn.setTransactionIsolationLevel(isolationLevel);
      }

      @Override
      public Publisher<Boolean> validate(ValidationDepth depth) {
        return conn.validate(depth);
      }

      @Override
      public String toString() {
        return "WrappedConnection[" + id + "]";
      }
    }
  }
}
