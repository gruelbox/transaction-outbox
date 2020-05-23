package com.gruelbox.transactionoutbox.r2dbc;

import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.r2dbc.R2dbcRawTransactionManager.ConnectionFactoryWrapper;
import com.gruelbox.transactionoutbox.spi.BaseTransactionManager;
import com.gruelbox.transactionoutbox.sql.AbstractSqlPersistorTest;
import com.gruelbox.transactionoutbox.sql.Dialect;
import io.r2dbc.h2.H2ConnectionConfiguration;
import io.r2dbc.h2.H2ConnectionFactory;
import io.r2dbc.spi.Connection;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import reactor.core.publisher.Hooks;

@Slf4j
class TestR2dbcPersistorH2 extends AbstractSqlPersistorTest<Connection, R2dbcRawTransaction> {

  private final R2dbcPersistor persistor = R2dbcPersistor.forDialect(Dialect.H2);
  private final ConnectionFactoryWrapper connectionFactory =
      R2dbcRawTransactionManager.wrapConnectionFactory(
          new H2ConnectionFactory(
              H2ConnectionConfiguration.builder()
                  .inMemory("test")
                  .username("test")
                  .password("test")
                  .option("DB_CLOSE_DELAY=-1")
                  .build()));
  private final R2dbcRawTransactionManager txManager =
      new R2dbcRawTransactionManager(connectionFactory);

  @BeforeAll
  static void initHooks() {
    Hooks.onOperatorDebug();
  }

  @Override
  protected Dialect dialect() {
    return Dialect.H2;
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
}
