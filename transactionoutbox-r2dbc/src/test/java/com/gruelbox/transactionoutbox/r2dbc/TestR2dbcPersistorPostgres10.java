package com.gruelbox.transactionoutbox.r2dbc;

import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.r2dbc.R2dbcRawTransactionManager.ConnectionFactoryWrapper;
import com.gruelbox.transactionoutbox.spi.BaseTransactionManager;
import com.gruelbox.transactionoutbox.sql.AbstractSqlPersistorTest;
import com.gruelbox.transactionoutbox.sql.Dialect;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.Connection;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Hooks;

@Slf4j
@Testcontainers
class TestR2dbcPersistorPostgres10
    extends AbstractSqlPersistorTest<Connection, R2dbcRawTransaction> {

  @Container
  @SuppressWarnings("rawtypes")
  private static final JdbcDatabaseContainer container =
      (JdbcDatabaseContainer)
          new PostgreSQLContainer("postgres:10").withStartupTimeout(Duration.ofHours(1));

  private final R2dbcPersistor persistor = R2dbcPersistor.forDialect(Dialect.POSTGRESQL_9);
  private final ConnectionFactoryWrapper connectionFactory =
      R2dbcRawTransactionManager.wrapConnectionFactory(
          new PostgresqlConnectionFactory(
              PostgresqlConnectionConfiguration.builder()
                  .host(container.getHost())
                  .username(container.getUsername())
                  .password(container.getPassword())
                  .port(container.getFirstMappedPort())
                  .database(container.getDatabaseName())
                  .build()));
  private final R2dbcRawTransactionManager txManager =
      new R2dbcRawTransactionManager(connectionFactory);

  @BeforeAll
  static void initHooks() {
    Hooks.onOperatorDebug();
  }

  @Override
  protected Dialect dialect() {
    return Dialect.POSTGRESQL_9;
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
    Assertions.assertEquals(0, txManager.getOpenTransactionCount());
  }
}
