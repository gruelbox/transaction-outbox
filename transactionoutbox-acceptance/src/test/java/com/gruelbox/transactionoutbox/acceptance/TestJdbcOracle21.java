package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.sql.Dialects;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.CompletionException;

@SuppressWarnings("WeakerAccess")
@Testcontainers
@Slf4j
class TestJdbcOracle21 extends AbstractSimpleTransactionManagerAcceptanceTest {
  @Container
  @SuppressWarnings("rawtypes")
  private static final JdbcDatabaseContainer container =
      new OracleContainer("gvenzl/oracle-xe:21-slim").withStartupTimeout(Duration.ofHours(1));

  @Override
  protected JdbcConnectionDetails connectionDetails() {
    return JdbcConnectionDetails.builder()
        .dialect(Dialects.ORACLE)
        .driverClassName("oracle.jdbc.OracleDriver")
        .url(container.getJdbcUrl())
        .user(container.getUsername())
        .password(container.getPassword())
        .build();
  }

  @Override
  protected final void prepareDataStore() {
    txManager
      .transactionally(tx -> runSql(tx, "CREATE TABLE TESTDATA(VAL INT)"))
      .exceptionally(
        t -> {
          if (t instanceof CompletionException) {
            t = t.getCause();
          }
          if (t instanceof SQLException && t.getMessage().contains("955")) {
            return null;
          }
          if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
          }
          throw new CompletionException(t);
        })
      .thenRun(() -> log.info("Table created"))
      .join();
  }
}
