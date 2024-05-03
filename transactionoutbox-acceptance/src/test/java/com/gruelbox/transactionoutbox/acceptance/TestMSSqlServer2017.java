package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.testing.AbstractAcceptanceTest;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;

@SuppressWarnings("WeakerAccess")
@Testcontainers
class TestMSSqlServer2017 extends AbstractAcceptanceTest {

  @Container
  @SuppressWarnings({"rawtypes", "resource"})
  private static final JdbcDatabaseContainer container =
      new MSSQLServerContainer<>("mcr.microsoft.com/mssql/server:2017-latest")
          .acceptLicense()
          .withStartupTimeout(Duration.ofMinutes(5))
          .withReuse(true);

  @Override
  protected ConnectionDetails connectionDetails() {
    return ConnectionDetails.builder()
        .dialect(Dialect.MS_SQL_SERVER)
        .driverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .url(container.getJdbcUrl())
        .user(container.getUsername())
        .password(container.getPassword())
        .build();
  }
}
