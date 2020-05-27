package com.gruelbox.transactionoutbox.jdbc;

import com.gruelbox.transactionoutbox.Dialect;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
class TestJdbcPersistorPostgres10 extends AbstractJdbcPersistorTest {

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
  protected JdbcPersistor persistor() {
    return persistor;
  }

  @Override
  protected SimpleTransactionManager txManager() {
    return txManager;
  }

  @Override
  protected Dialect dialect() {
    return Dialect.POSTGRESQL_9;
  }
}
