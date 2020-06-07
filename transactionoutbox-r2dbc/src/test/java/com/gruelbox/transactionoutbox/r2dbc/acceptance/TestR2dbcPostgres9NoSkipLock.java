package com.gruelbox.transactionoutbox.r2dbc.acceptance;

import com.gruelbox.transactionoutbox.sql.Dialect;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;
import java.time.Duration;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class TestR2dbcPostgres9NoSkipLock extends AbstractR2dbcAcceptanceTest {

  @Container
  @SuppressWarnings("rawtypes")
  private static final JdbcDatabaseContainer container =
      (JdbcDatabaseContainer)
          new PostgreSQLContainer("postgres:9").withStartupTimeout(Duration.ofHours(1));

  @SuppressWarnings("deprecation")
  @Override
  protected Dialect dialect() {
    return Dialect.POSTGRESQL__TEST_NO_SKIP_LOCK;
  }

  @Override
  protected ConnectionFactory createConnectionFactory() {
    return new PostgresqlConnectionFactory(
        PostgresqlConnectionConfiguration.builder()
            .host(container.getHost())
            .username(container.getUsername())
            .password(container.getPassword())
            .port(container.getFirstMappedPort())
            .database(container.getDatabaseName())
            .build());
  }
}
