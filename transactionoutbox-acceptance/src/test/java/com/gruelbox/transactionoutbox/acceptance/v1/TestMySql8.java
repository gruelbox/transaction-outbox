package com.gruelbox.transactionoutbox.acceptance.v1;

import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.acceptance.JdbcConnectionDetails;
import java.time.Duration;
import java.util.Map;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class TestMySql8 extends AbstractAcceptanceTestV1 {

  @Container
  @SuppressWarnings("rawtypes")
  private static final JdbcDatabaseContainer container =
      new MySQLContainer<>("mysql:8").withStartupTimeout(Duration.ofHours(1))
        .withReuse(true)
        .withTmpFs(Map.of("/var/lib/mysql", "rw,noexec,nosuid,size=512m"));

  @SuppressWarnings("deprecation")
  @Override
  protected JdbcConnectionDetails connectionDetails() {
    return JdbcConnectionDetails.builder()
        .dialect(Dialect.MY_SQL_8)
        .driverClassName("com.mysql.cj.jdbc.Driver")
        .url(container.getJdbcUrl())
        .user(container.getUsername())
        .password(container.getPassword())
        .build();
  }
}
