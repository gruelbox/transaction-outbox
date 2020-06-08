package com.gruelbox.transactionoutbox.r2dbc.acceptance;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import com.gruelbox.transactionoutbox.acceptance.AbstractSqlAcceptanceTest;
import com.gruelbox.transactionoutbox.r2dbc.R2dbcPersistor;
import com.gruelbox.transactionoutbox.r2dbc.R2dbcRawTransactionManager;
import com.gruelbox.transactionoutbox.r2dbc.R2dbcRawTransactionManager.ConnectionFactoryWrapper;
import com.gruelbox.transactionoutbox.r2dbc.R2dbcTransaction;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;

@Slf4j
abstract class AbstractR2dbcAcceptanceTest
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
  void checkOpenTransactions() {
    assertThat("Should be no open transactions at the start of the test",
        txManager.getOpenTransactionCount(), equalTo(0));
  }

  @Override
  protected final R2dbcRawTransactionManager createTxManager() {
    ConnectionPoolConfiguration configuration =
        ConnectionPoolConfiguration.builder(createConnectionFactory())
            .maxIdleTime(Duration.ofSeconds(10))
            .maxSize(20)
            .build();
    ConnectionPool pool = autoClose(new ConnectionPool(configuration));
    ConnectionFactoryWrapper connectionFactory =
        R2dbcRawTransactionManager.wrapConnectionFactory(pool);
    return new R2dbcRawTransactionManager(connectionFactory);
  }

  @Override
  protected CompletableFuture<Void> scheduleWithTx(
      TransactionOutbox outbox, R2dbcTransaction tx, int arg1, String arg2) {
    return outbox.schedule(InterfaceProcessor.class).processAsync(arg1, arg2, tx);
  }

  @Override
  protected CompletableFuture<Void> scheduleWithCtx(
      TransactionOutbox outbox, Object context, int arg1, String arg2) {
    return outbox.schedule(InterfaceProcessor.class).processAsync(arg1, arg2, (Connection) context);
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
