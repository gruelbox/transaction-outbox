package com.gruelbox.transactionoutbox.acceptance.persistor;

import com.gruelbox.transactionoutbox.DefaultPersistor;
import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.TransactionManager;
import com.gruelbox.transactionoutbox.testing.AbstractPersistorTest;
import java.time.Duration;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class TestDefaultPersistorPostgres16 extends AbstractPersistorTest {

  @Container
  @SuppressWarnings({"rawtypes", "resource"})
  private static final JdbcDatabaseContainer container =
      (JdbcDatabaseContainer)
          new PostgreSQLContainer("postgres:16").withStartupTimeout(Duration.ofHours(1));

  private final DefaultPersistor persistor =
      DefaultPersistor.builder().dialect(Dialect.POSTGRESQL_9).build();
  private final TransactionManager txManager =
      TransactionManager.fromConnectionDetails(
          "org.postgresql.Driver",
          container.getJdbcUrl(),
          container.getUsername(),
          container.getPassword());

  @Override
  protected DefaultPersistor persistor() {
    return persistor;
  }

  @Override
  protected TransactionManager txManager() {
    return txManager;
  }

  @Override
  protected Dialect dialect() {
    return Dialect.POSTGRESQL_9;
  }
}
