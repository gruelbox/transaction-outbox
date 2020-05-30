package com.gruelbox.transactionoutbox.r2dbc;


import com.gruelbox.transactionoutbox.AbstractSqlPersistor;
import com.gruelbox.transactionoutbox.AbstractSqlPersistorTest;
import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.TransactionManager;
import com.gruelbox.transactionoutbox.r2dbc.R2dbcRawTransactionManager.ConnectionFactoryWrapper;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.Connection;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

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

  @Override
  protected Dialect dialect() {
    return Dialect.POSTGRESQL_9;
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
