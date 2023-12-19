package com.gruelbox.transactionoutbox.jooq.acceptance;

import com.gruelbox.transactionoutbox.Dialect;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.jooq.SQLDialect;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
class TestJooqThreadLocalPostgres16 extends AbstractJooqAcceptanceThreadLocalTest {

  @Container
  @SuppressWarnings({"rawtypes", "resource"})
  private static final JdbcDatabaseContainer container =
      (JdbcDatabaseContainer)
          new PostgreSQLContainer("postgres:16").withStartupTimeout(Duration.ofHours(1));

  @Override
  protected ConnectionDetails connectionDetails() {
    return ConnectionDetails.builder()
        .dialect(Dialect.POSTGRESQL_9)
        .driverClassName("org.postgresql.Driver")
        .url(container.getJdbcUrl())
        .user(container.getUsername())
        .password(container.getPassword())
        .build();
  }

  @Override
  protected SQLDialect jooqDialect() {
    return SQLDialect.POSTGRES;
  }
}
