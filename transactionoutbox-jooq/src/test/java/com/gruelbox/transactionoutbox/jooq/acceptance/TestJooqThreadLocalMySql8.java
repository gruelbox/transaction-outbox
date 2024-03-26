package com.gruelbox.transactionoutbox.jooq.acceptance;

import com.gruelbox.transactionoutbox.*;
import java.time.Duration;
import java.util.Map;
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
  @SuppressWarnings({"rawtypes", "resource", "unchecked"})
  private static final JdbcDatabaseContainer<?> container =
      (JdbcDatabaseContainer<?>)
          new MySQLContainer("mysql:8")
              .withStartupTimeout(Duration.ofMinutes(5))
              .withReuse(true)
              .withTmpFs(Map.of("/var/lib/mysql", "rw"));

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
