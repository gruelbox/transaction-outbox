package com.gruelbox.transactionoutbox.testing;

import java.time.Duration;
import java.util.Map;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.PostgreSQLContainer;

public class TestContainerUtils {
  private TestContainerUtils() {}

  @SuppressWarnings({"rawtypes", "resource"})
  public static JdbcDatabaseContainer getMySql5Container() {
    return new MySQLContainer<>("mysql:5")
        .withStartupTimeout(Duration.ofMinutes(5))
        .withReuse(true)
        .withTmpFs(Map.of("/var/lib/mysql", "rw"));
  }

  @SuppressWarnings({"rawtypes", "resource"})
  public static JdbcDatabaseContainer getMySql8Container() {
    return new MySQLContainer<>("mysql:8")
        .withStartupTimeout(Duration.ofMinutes(5))
        .withReuse(true)
        .withTmpFs(Map.of("/var/lib/mysql", "rw"));
  }

  @SuppressWarnings({"rawtypes", "resource"})
  public static JdbcDatabaseContainer getPostgres16Container() {
    return (JdbcDatabaseContainer)
        new PostgreSQLContainer("postgres:16")
            .withStartupTimeout(Duration.ofHours(1))
            .withReuse(true);
  }

  @SuppressWarnings({"rawtypes", "resource"})
  public static JdbcDatabaseContainer getOracle18Container() {
    return new OracleContainer("gvenzl/oracle-xe:18-slim-faststart")
        .withStartupTimeout(Duration.ofHours(1))
        .withReuse(true);
  }

  @SuppressWarnings({"rawtypes", "resource"})
  public static JdbcDatabaseContainer getMSSQL2017Container() {
    return new MSSQLServerContainer<>("mcr.microsoft.com/mssql/server:2017-latest")
        .acceptLicense()
        .withStartupTimeout(Duration.ofMinutes(5))
        .withReuse(true);
  }
}
