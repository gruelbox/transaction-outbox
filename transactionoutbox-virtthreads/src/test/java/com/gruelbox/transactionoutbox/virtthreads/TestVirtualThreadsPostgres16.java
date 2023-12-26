package com.gruelbox.transactionoutbox.virtthreads;

import static com.gruelbox.transactionoutbox.Dialect.POSTGRESQL_9;

import java.time.Duration;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SuppressWarnings("WeakerAccess")
@Testcontainers
class TestVirtualThreadsPostgres16 extends AbstractVirtualThreadsTest {

  @Container
  @SuppressWarnings({"rawtypes", "resource"})
  private static final JdbcDatabaseContainer container =
      (JdbcDatabaseContainer)
          new PostgreSQLContainer("postgres:16").withStartupTimeout(Duration.ofHours(1));

  @Override
  protected ConnectionDetails connectionDetails() {
    return ConnectionDetails.builder()
        .dialect(POSTGRESQL_9)
        .driverClassName("org.postgresql.Driver")
        .url(container.getJdbcUrl())
        .user(container.getUsername())
        .password(container.getPassword())
        .build();
  }
}
