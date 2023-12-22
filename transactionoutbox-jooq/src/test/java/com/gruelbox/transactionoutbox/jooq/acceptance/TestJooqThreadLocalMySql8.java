package com.gruelbox.transactionoutbox.jooq.acceptance;

import com.gruelbox.transactionoutbox.*;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.jooq.SQLDialect;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
class TestJooqThreadLocalMySql8 extends AbstractJooqAcceptanceThreadLocalTest {

  @Container
  @SuppressWarnings({"rawtypes", "resource"})
  private static final JdbcDatabaseContainer<?> container =
      (JdbcDatabaseContainer<?>)
          new MySQLContainer("mysql:8").withStartupTimeout(Duration.ofMinutes(5));

  @Override
  protected ConnectionDetails connectionDetails() {
    return ConnectionDetails.builder()
        .dialect(Dialect.MY_SQL_8)
        .driverClassName("com.mysql.cj.jdbc.Driver")
        .url(container.getJdbcUrl())
        .user(container.getUsername())
        .password(container.getPassword())
        .build();
  }

  @Override
  protected SQLDialect jooqDialect() {
    return SQLDialect.MYSQL;
  }
}
