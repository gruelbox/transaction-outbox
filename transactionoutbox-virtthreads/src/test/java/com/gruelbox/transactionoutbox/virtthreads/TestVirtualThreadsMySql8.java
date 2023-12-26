package com.gruelbox.transactionoutbox.virtthreads;

import com.gruelbox.transactionoutbox.Dialect;
import java.time.Duration;
import org.junit.jupiter.api.Disabled;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SuppressWarnings("WeakerAccess")
@Testcontainers
@Disabled
class TestVirtualThreadsMySql8 extends AbstractVirtualThreadsTest {

  @Container
  @SuppressWarnings({"rawtypes", "resource"})
  private static final JdbcDatabaseContainer container =
      new MySQLContainer<>("mysql:8").withStartupTimeout(Duration.ofMinutes(5));

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
}
