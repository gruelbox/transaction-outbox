package com.gruelbox.transactionoutbox.acceptance;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;

import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.SchedulerProxyFactory;
import com.gruelbox.transactionoutbox.r2dbc.R2dbcPersistor;
import com.gruelbox.transactionoutbox.r2dbc.R2dbcRawTransactionManager;
import com.gruelbox.transactionoutbox.r2dbc.R2dbcRawTransactionManager.ConnectionFactoryWrapper;
import com.gruelbox.transactionoutbox.r2dbc.R2dbcTransaction;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

@Slf4j
public abstract class AbstractR2dbcAcceptanceTest
    extends AbstractSqlAcceptanceTest<Connection, R2dbcTransaction, R2dbcRawTransactionManager> {

  protected abstract ConnectionFactory createConnectionFactory();

  @Override
  protected Persistor<Connection, R2dbcTransaction> createPersistor() {
    return R2dbcPersistor.forDialect(dialect());
  }

  @BeforeAll
  static void initHooks() {
    Hooks.onOperatorDebug();
  }

  @BeforeEach
  void checkOpenTransactionsBefore() {
    assertThat(
        "Should be no open transactions at the start of the test",
        txManager.getOpenTransactions(),
        empty());
  }

  @AfterEach
  void checkOpenTransactionsAfter() {
    assertThat(
        "Should be no open transactions at the end of the test",
        txManager.getOpenTransactions(),
        empty());
  }

  @Override
  protected final R2dbcRawTransactionManager createTxManager() {
    ConnectionPoolConfiguration configuration =
        ConnectionPoolConfiguration.builder(createConnectionFactory())
            .maxIdleTime(Duration.ofSeconds(10))
            .maxSize(20)
            .build();
    ConnectionPool pool = new ConnectionPool(configuration);
    autoClose(() -> pool.close().block());
    ConnectionFactoryWrapper connectionFactory =
        R2dbcRawTransactionManager.wrapConnectionFactory(pool);
    return new R2dbcRawTransactionManager(connectionFactory).enableStackLogging();
  }

  @Override
  protected CompletableFuture<Void> scheduleWithTx(
      SchedulerProxyFactory outbox, R2dbcTransaction tx, int arg1, String arg2) {
    return outbox.schedule(AsyncInterfaceProcessor.class).processAsync(arg1, arg2, tx);
  }

  @Override
  protected CompletableFuture<Void> scheduleWithCtx(
      SchedulerProxyFactory outbox, Object context, int arg1, String arg2) {
    return outbox.schedule(AsyncInterfaceProcessor.class).processAsync(arg1, arg2, (Connection) context);
  }

  @Override
  protected CompletableFuture<?> runSql(Object txOrContext, String sql) {
    Connection connection;
    if (txOrContext instanceof R2dbcTransaction) {
      connection = ((R2dbcTransaction) txOrContext).connection();
    } else {
      connection = (Connection) txOrContext;
    }
    return Mono.from(connection.createStatement(sql).execute())
        .flatMap(result -> Mono.from(result.getRowsUpdated()))
        .toFuture();
  }

  @Override
  protected CompletableFuture<Long> readLongValue(R2dbcTransaction tx, String sql) {
    return Mono.from(tx.connection().createStatement(sql).execute())
        .flatMapMany(result -> Flux.from(result.map((r, m) -> r.get(0, Long.class))))
        .next()
        .toFuture();
  }
}
