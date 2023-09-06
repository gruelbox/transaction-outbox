package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.DialectSqlMySQL8Impl;
import java.time.Duration;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SuppressWarnings("WeakerAccess")
@Testcontainers
class TestMySql8 extends AbstractAcceptanceTest {

  @Container
  @SuppressWarnings("rawtypes")
  private static final JdbcDatabaseContainer container =
      new MySQLContainer<>("mysql:8.0.27").withStartupTimeout(Duration.ofMinutes(5));

  @Override
  protected ConnectionDetails connectionDetails() {
    return ConnectionDetails.builder()
        .dialect(new DialectSqlMySQL8Impl())
        .driverClassName("com.mysql.cj.jdbc.Driver")
        .url(container.getJdbcUrl())
        .user(container.getUsername())
        .password(container.getPassword())
        .build();
  }
}
