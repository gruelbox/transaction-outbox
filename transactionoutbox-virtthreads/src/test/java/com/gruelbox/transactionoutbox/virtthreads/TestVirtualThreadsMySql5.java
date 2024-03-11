package com.gruelbox.transactionoutbox.virtthreads;

import static com.gruelbox.transactionoutbox.Dialect.MY_SQL_5;

import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Disabled;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SuppressWarnings("WeakerAccess")
@Testcontainers
@Disabled
class TestVirtualThreadsMySql5 extends AbstractVirtualThreadsTest {

  @Container
  @SuppressWarnings({"rawtypes", "resource"})
  private static final JdbcDatabaseContainer container =
      new MySQLContainer<>("mysql:5")
          .withStartupTimeout(Duration.ofMinutes(5))
          .withTmpFs(Map.of("/var/lib/mysql", "rw"));

  @Override
  protected ConnectionDetails connectionDetails() {
    return ConnectionDetails.builder()
        .dialect(MY_SQL_5)
        .driverClassName("com.mysql.cj.jdbc.Driver")
        .url(container.getJdbcUrl())
        .user(container.getUsername())
        .password(container.getPassword())
        .build();
  }
}
