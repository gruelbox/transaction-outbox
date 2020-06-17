package com.gruelbox.transactionoutbox.r2dbc;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import java.time.Duration;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public interface UsesPostgres12 {

  @Container
  @SuppressWarnings("rawtypes")
  JdbcDatabaseContainer CONTAINER =
      (JdbcDatabaseContainer)
          new PostgreSQLContainer("postgres:12")
              .withStartupTimeout(Duration.ofHours(1))
              .withReuse(true);

  static PostgresqlConnectionConfiguration connectionConfiguration() {
    return PostgresqlConnectionConfiguration.builder()
        .host(CONTAINER.getHost())
        .username(CONTAINER.getUsername())
        .password(CONTAINER.getPassword())
        .port(CONTAINER.getFirstMappedPort())
        .database(CONTAINER.getDatabaseName())
        .build();
  }
}
