package com.gruelbox.transactionoutbox;

import java.time.Duration;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class TestDefaultPersistorOracle18 extends AbstractDefaultPersistorTest {

  @Container
  @SuppressWarnings("rawtypes")
  private static final JdbcDatabaseContainer container =
      (JdbcDatabaseContainer)
          new OracleContainer("gvenzl/oracle-xe:18-slim").withStartupTimeout(Duration.ofHours(1));

  private DefaultPersistor persistor = DefaultPersistor.builder().dialect(Dialect.ORACLE).build();

  private TransactionManager txManager =
      TransactionManager.fromConnectionDetails(
          "oracle.jdbc.OracleDriver",
          container.getJdbcUrl(),
          container.getUsername(),
          container.getPassword());

  @Override
  protected DefaultPersistor persistor() {
    return persistor;
  }

  @Override
  protected TransactionManager txManager() {
    return txManager;
  }

  @Override
  protected Dialect dialect() {
    return Dialect.ORACLE;
  }
}
