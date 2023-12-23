package com.gruelbox.transactionoutbox.virtthreads;

import com.gruelbox.transactionoutbox.Dialect;
import java.time.Duration;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SuppressWarnings("WeakerAccess")
@Testcontainers
class TestVirtualThreadsOracle21 extends AbstractVirtualThreadsTest {

  @Container
  @SuppressWarnings("rawtypes")
  private static final JdbcDatabaseContainer container =
      new OracleContainer("gvenzl/oracle-xe:21-slim").withStartupTimeout(Duration.ofHours(1));

  @Override
  protected ConnectionDetails connectionDetails() {
    return ConnectionDetails.builder()
        .dialect(Dialect.ORACLE)
        .driverClassName("oracle.jdbc.OracleDriver")
        .url(container.getJdbcUrl())
        .user(container.getUsername())
        .password(container.getPassword())
        .build();
  }
}
