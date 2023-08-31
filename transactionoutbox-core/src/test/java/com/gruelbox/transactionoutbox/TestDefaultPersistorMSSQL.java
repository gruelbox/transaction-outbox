package com.gruelbox.transactionoutbox;

import java.time.Duration;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class TestDefaultPersistorMSSQL extends AbstractDefaultPersistorTest {

  @Container
  @SuppressWarnings("rawtypes")
  private static final JdbcDatabaseContainer container =
      (JdbcDatabaseContainer)
          new MSSQLServerContainer("mcr.microsoft.com/mssql/server:2017-latest")
              .withStartupTimeout(Duration.ofHours(1));

  private final DefaultPersistor persistor =
      DefaultPersistor.builder().dialect(Dialect.MSSQL).build();

  private TransactionManager txManager =
      TransactionManager.fromConnectionDetails(
          "com.microsoft.sqlserver.jdbc.SQLServerDriver",
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
    return Dialect.MSSQL;
  }
}
