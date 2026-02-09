package com.gruelbox.transactionoutbox.virtthreads;

import static com.gruelbox.transactionoutbox.Dialect.MY_SQL_8;

import java.time.Duration;
import java.util.Map;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SuppressWarnings("WeakerAccess")
@Testcontainers
class TestVirtualThreadsMySql8 extends AbstractVirtualThreadsTest {

  @Container
  @SuppressWarnings({"rawtypes", "resource"})
  private static final JdbcDatabaseContainer container =
      new MySQLContainer<>("mysql:8")
          .withStartupTimeout(Duration.ofMinutes(5))
          .withReuse(true)
          .withTmpFs(Map.of("/var/lib/mysql", "rw"));

  @Override
  protected ConnectionDetails connectionDetails() {
    return ConnectionDetails.builder()
        .dialect(MY_SQL_8)
        .driverClassName("com.mysql.cj.jdbc.Driver")
        .url(container.getJdbcUrl())
        .user(container.getUsername())
        .password(container.getPassword())
        .build();
  }
}
