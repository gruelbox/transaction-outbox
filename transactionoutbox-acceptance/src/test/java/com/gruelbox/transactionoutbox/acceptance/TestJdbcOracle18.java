package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.sql.Dialects;
import java.time.Duration;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SuppressWarnings("WeakerAccess")
@Testcontainers
class TestJdbcOracle18 extends AbstractSimpleTransactionManagerAcceptanceTest {

  @Container
  @SuppressWarnings("rawtypes")
  private static final JdbcDatabaseContainer container =
      new OracleContainer("gvenzl/oracle-xe:18-slim").withStartupTimeout(Duration.ofHours(1));

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
}
