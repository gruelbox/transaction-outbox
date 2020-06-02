package com.gruelbox.transactionoutbox.jdbc;

import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.sql.AbstractSqlPersistorTest;
import com.gruelbox.transactionoutbox.sql.Dialect;
import java.sql.Connection;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
class TestJdbcPersistorPostgres10
    extends AbstractSqlPersistorTest<Connection, SimpleTransaction<Void>> {

  @Container
  @SuppressWarnings("rawtypes")
  private static final JdbcDatabaseContainer container =
      (JdbcDatabaseContainer)
          new PostgreSQLContainer("postgres:10").withStartupTimeout(Duration.ofHours(1));

  private JdbcPersistor persistor = JdbcPersistor.builder().dialect(Dialect.POSTGRESQL_9).build();
  private SimpleTransactionManager txManager =
      SimpleTransactionManager.fromConnectionDetails(
          "org.postgresql.Driver",
          container.getJdbcUrl(),
          container.getUsername(),
          container.getPassword());

  @Override
  protected Dialect dialect() {
    return Dialect.POSTGRESQL_9;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Persistor<Connection, SimpleTransaction<Void>> persistor() {
    return (Persistor) persistor;
  }

  @Override
  protected SimpleTransactionManager txManager() {
    return txManager;
  }
}
