package com.gruelbox.transactionoutbox.r2dbc;

import com.gruelbox.transactionoutbox.AbstractSqlPersistor;
import com.gruelbox.transactionoutbox.AbstractSqlPersistorTest;
import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.TransactionManager;
import com.gruelbox.transactionoutbox.r2dbc.R2dbcRawTransactionManager.ConnectionFactoryWrapper;
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
  protected AbstractSqlPersistor<Connection, R2dbcRawTransaction> persistor() {
    Persistor result = persistor;
    return (AbstractSqlPersistor<Connection, R2dbcRawTransaction>) result;
  }

  @Override
  protected TransactionManager<Connection, ?, R2dbcRawTransaction> txManager() {
    return txManager;
  }
}
