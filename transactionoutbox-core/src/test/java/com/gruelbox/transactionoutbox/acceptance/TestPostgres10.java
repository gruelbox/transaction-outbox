package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.DialectSqlPostgresImpl;
import java.time.Duration;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SuppressWarnings("WeakerAccess")
@Testcontainers
class TestPostgres10 extends AbstractAcceptanceTest {

  @Container
  @SuppressWarnings("rawtypes")
  private static final JdbcDatabaseContainer container =
      (JdbcDatabaseContainer)
          new PostgreSQLContainer("postgres:10").withStartupTimeout(Duration.ofHours(1));

  @Override
  protected ConnectionDetails connectionDetails() {
    return ConnectionDetails.builder()
        .dialect(new DialectSqlPostgresImpl())
        .driverClassName("org.postgresql.Driver")
        .url(container.getJdbcUrl())
        .user(container.getUsername())
        .password(container.getPassword())
        .build();
  }
}
