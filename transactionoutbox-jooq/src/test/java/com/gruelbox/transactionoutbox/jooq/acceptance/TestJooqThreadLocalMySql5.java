package com.gruelbox.transactionoutbox.jooq.acceptance;

import com.gruelbox.transactionoutbox.Dialect;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.jooq.SQLDialect;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
class TestJooqThreadLocalMySql5 extends AbstractJooqAcceptanceThreadLocalTest {

  @Container
  @SuppressWarnings({"rawtypes", "resource"})
  private static final JdbcDatabaseContainer<?> container =
      (JdbcDatabaseContainer<?>)
          new MySQLContainer("mysql:5").withStartupTimeout(Duration.ofMinutes(5));

  @Override
  protected ConnectionDetails connectionDetails() {
    return ConnectionDetails.builder()
        .dialect(Dialect.MY_SQL_5)
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
