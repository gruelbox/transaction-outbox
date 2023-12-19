package com.gruelbox.transactionoutbox.acceptance.persistor;

import com.gruelbox.transactionoutbox.DefaultPersistor;
import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.TransactionManager;
import java.time.Duration;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import persistor.AbstractPersistorTest;

@Testcontainers
class TestDefaultPersistorOracle18 extends AbstractPersistorTest {

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
