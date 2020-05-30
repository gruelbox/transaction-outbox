package com.gruelbox.transactionoutbox.r2dbc;

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.ConnectionMetadata;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.ValidationDepth;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * A transaction manager which uses the raw R2DBC {@link Connection} SPI as its context. This is
 * unlikely to be particularly useful in client code where a particular high-level R2DBC-compatible
 * API will be in use. It's provided mainly as a proof of concept.
 */
@Slf4j
public class R2dbcRawTransactionManager
    implements R2dbcTransactionManager<Connection, R2dbcRawTransaction> {

  private final ConnectionFactoryWrapper cf;
  private final AtomicInteger openTransactionCount = new AtomicInteger();

  public R2dbcRawTransactionManager(ConnectionFactoryWrapper cf) {
    this.cf = cf;
  }

  public static ConnectionFactoryWrapper wrapConnectionFactory(
      ConnectionFactory connectionFactory) {
    return new ConnectionFactoryWrapper(connectionFactory);
  }

  // For testing
  int getOpenTransactionCount() {
    return openTransactionCount.get();
  }

  @Override
  public <T> CompletableFuture<T> transactionally(
      Function<R2dbcRawTransaction, CompletableFuture<T>> fn) {
    return withConnection(
            conn ->
                Mono.from(conn.beginTransaction())
                    .then(Mono.fromCompletionStage(fn.apply(transactionFromContext(conn))))
                    .concatWith(commit(conn))
                    .onErrorResume(t -> rollback(conn, t))
                    .next())
        .toFuture();
  }

  private <T> Mono<T> commit(Connection conn) {
    return Mono.from(conn.commitTransaction()).then(Mono.empty());
  }

  private <T> Mono<T> rollback(Connection conn, Throwable e) {
    return Mono.fromRunnable(
            () ->
                log.warn(
                    "Exception in transactional block ({}{}). Rolling back. See later messages for detail",
                    e.getClass().getSimpleName(),
                    e.getMessage() == null ? "" : (" - " + e.getMessage())))
        .concatWith(conn.rollbackTransaction())
        .then(Mono.error(e));
  }

  private <T> Mono<T> withConnection(Function<Connection, Mono<T>> fn) {
    return Mono.from(cf.create())
        .flatMapMany(
            conn ->
                Mono.fromRunnable(openTransactionCount::incrementAndGet)
                    .then(fn.apply(conn))
                    .concatWith(
                        Flux.from(conn.close())
                            .then(Mono.fromRunnable(openTransactionCount::decrementAndGet)))
                    .onErrorResume(
                        t ->
                            Flux.from(conn.close())
                                .then(Mono.fromRunnable(openTransactionCount::decrementAndGet))
                                .then(Mono.error(t))))
        .next();
  }

  @Override
  public R2dbcRawTransaction transactionFromContext(Connection connection) {
    return cf.contextMap.get(connection);
  }

  @Override
  public Class<Connection> contextType() {
    return Connection.class;
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
                log.debug("Adding transaction for connection {} to map", conn);
                contextMap.put(conn, new R2dbcRawTransaction(conn));
                return conn;
              });
    }

    @Override
    public ConnectionFactoryMetadata getMetadata() {
      return delegate.getMetadata();
    }

    @SuppressWarnings("NullableProblems")
    private class WrappedConnection implements Connection {

      private final Connection conn;

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
                      log.debug("Removing transaction for connection {} from map", this);
                      contextMap.remove(this);
                    }));
      }

      @Override
      public Publisher<Void> commitTransaction() {
        return Mono.from(conn.commitTransaction())
            .then(Mono.fromCompletionStage(contextMap.get(this).processHooks()));
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
    }
  }
}
