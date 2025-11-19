package com.gruelbox.transactionoutbox.jooq.acceptance;

import com.gruelbox.transactionoutbox.Dialect;
import java.time.Duration;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.jooq.SQLDialect;
import org.junit.Ignore;
import org.junit.jupiter.api.Disabled;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
@Ignore
@Disabled
class TestJooqThreadLocalMySql5 extends AbstractJooqAcceptanceThreadLocalTest {

  @Container
  @SuppressWarnings({"rawtypes", "resource", "unchecked"})
  private static final JdbcDatabaseContainer<?> container =
      (JdbcDatabaseContainer<?>)
          new MySQLContainer("mysql:5")
              .withStartupTimeout(Duration.ofMinutes(5))
              .withReuse(true)
              .withTmpFs(Map.of("/var/lib/mysql", "rw"));

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
