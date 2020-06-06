package com.gruelbox.transactionoutbox.r2dbc.acceptance;

import com.gruelbox.transactionoutbox.sql.Dialect;
import dev.miku.r2dbc.mysql.MySqlConnectionConfiguration;
import dev.miku.r2dbc.mysql.MySqlConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;
import java.time.Duration;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class TestR2dbcMySql8 extends AbstractR2dbcAcceptanceTest {

  @Container
  @SuppressWarnings("rawtypes")
  private static final JdbcDatabaseContainer container =
      new MySQLContainer<>("mysql:8").withStartupTimeout(Duration.ofHours(1));

  @Override
  protected Dialect dialect() {
    return Dialect.MY_SQL_5;
  }

  @Override
  protected ConnectionFactory createConnectionFactory() {
    return MySqlConnectionFactory.from(
        MySqlConnectionConfiguration.builder()
            .host(container.getHost())
            .username(container.getUsername())
            .password(container.getPassword())
            .port(container.getFirstMappedPort())
            .database(container.getDatabaseName())
            .build());
  }
}
