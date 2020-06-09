package com.gruelbox.transactionoutbox.r2dbc;

import static com.gruelbox.transactionoutbox.r2dbc.UsesPostgres12.connectionConfiguration;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;

import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.r2dbc.R2dbcRawTransactionManager.ConnectionFactoryWrapper;
import com.gruelbox.transactionoutbox.spi.BaseTransactionManager;
import com.gruelbox.transactionoutbox.sql.AbstractPersistorTest;
import com.gruelbox.transactionoutbox.sql.Dialect;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.Connection;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import reactor.core.publisher.Hooks;

@Slf4j
class TestR2dbcPersistorPostgres12NoSkipLock
    extends AbstractPersistorTest<Connection, R2dbcRawTransaction> implements UsesPostgres12 {

  @SuppressWarnings("deprecation")
  private final R2dbcPersistor persistor =
      R2dbcPersistor.forDialect(Dialect.POSTGRESQL__TEST_NO_SKIP_LOCK);

  private final ConnectionFactoryWrapper connectionFactory =
      R2dbcRawTransactionManager.wrapConnectionFactory(
          new PostgresqlConnectionFactory(connectionConfiguration()));
  private final R2dbcRawTransactionManager txManager =
      new R2dbcRawTransactionManager(connectionFactory);

  @BeforeAll
  static void initHooks() {
    Hooks.onOperatorDebug();
  }

  @SuppressWarnings("deprecation")
  @Override
  protected Dialect dialect() {
    return Dialect.POSTGRESQL__TEST_NO_SKIP_LOCK;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Persistor<Connection, R2dbcRawTransaction> persistor() {
    return (Persistor) persistor;
  }

  @Override
  protected BaseTransactionManager<Connection, R2dbcRawTransaction> txManager() {
    return txManager;
  }

  @Override
  protected void validateState() {
    assertThat(txManager.getOpenTransactions(), empty());
  }
}
