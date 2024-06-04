package com.gruelbox.transactionoutbox.acceptance.persistor;

import com.gruelbox.transactionoutbox.DefaultPersistor;
import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.TransactionManager;
import com.gruelbox.transactionoutbox.testing.AbstractPersistorTest;
import java.time.Duration;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class TestDefaultPersistorMSSqlServer2017 extends AbstractPersistorTest {

  @Container
  @SuppressWarnings({"rawtypes", "resource"})
  private static final JdbcDatabaseContainer container =
      new MSSQLServerContainer<>("mcr.microsoft.com/mssql/server:2017-latest")
          .acceptLicense()
          .withStartupTimeout(Duration.ofMinutes(5))
          .withReuse(true);

  private final DefaultPersistor persistor =
      DefaultPersistor.builder().dialect(Dialect.MS_SQL_SERVER).build();
  private final TransactionManager txManager =
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
    return Dialect.MS_SQL_SERVER;
  }
}
