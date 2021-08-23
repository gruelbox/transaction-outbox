package com.gruelbox.transactionoutbox.r2dbc;

import dev.miku.r2dbc.mysql.MySqlConnectionConfiguration;
import java.time.Duration;
import java.util.Map;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public interface UsesMySql8 {

  @Container
  @SuppressWarnings("rawtypes")
  JdbcDatabaseContainer CONTAINER =
      new MySQLContainer<>("mysql:8")
          .withStartupTimeout(Duration.ofHours(1))
          .withTmpFs(Map.of("/var/lib/mysql", "rw,noexec,nosuid,size=512m"));

  static MySqlConnectionConfiguration connectionConfiguration() {
    return MySqlConnectionConfiguration.builder()
        .host(CONTAINER.getHost())
        .username(CONTAINER.getUsername())
        .password(CONTAINER.getPassword())
        .port(CONTAINER.getFirstMappedPort())
        .database(CONTAINER.getDatabaseName())
        .build();
  }
}
