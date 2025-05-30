package com.gruelbox.transactionoutbox.jooq.acceptance;

import com.gruelbox.transactionoutbox.Dialect;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.jooq.SQLDialect;
import org.junit.Ignore;
import org.junit.jupiter.api.Disabled;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
@Ignore
@Disabled
class TestJooqThreadLocalMSSqlServer2019 extends AbstractJooqAcceptanceThreadLocalTest {

  @Container
  @SuppressWarnings({"rawtypes", "resource", "unchecked"})
  private static final JdbcDatabaseContainer<?> container =
      new MSSQLServerContainer<>("mcr.microsoft.com/mssql/server:2019-latest")
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

  @Override
  protected SQLDialect jooqDialect() {
    return SQLDialect.DEFAULT;
  }
}
