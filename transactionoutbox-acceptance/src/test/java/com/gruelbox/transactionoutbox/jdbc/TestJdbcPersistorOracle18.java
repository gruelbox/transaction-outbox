package com.gruelbox.transactionoutbox.jdbc;

import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.sql.AbstractPersistorTest;
import com.gruelbox.transactionoutbox.sql.Dialects;
import java.sql.Connection;
import java.time.Duration;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class TestJdbcPersistorOracle18 extends AbstractPersistorTest<Connection, SimpleTransaction<Void>> {

  private final JdbcPersistor persistor = JdbcPersistor.builder().dialect(Dialects.ORACLE).build();

  @Container
  @SuppressWarnings("rawtypes")
  private static final JdbcDatabaseContainer container =
      new OracleContainer("gvenzl/oracle-xe:18-slim").withStartupTimeout(Duration.ofHours(1));

  private final SimpleTransactionManager txManager =
      SimpleTransactionManager.fromConnectionDetails(
          "oracle.jdbc.OracleDriver",
          container.getJdbcUrl(),
          container.getUsername(),
          container.getPassword());

  @Override
  protected com.gruelbox.transactionoutbox.sql.Dialect dialect() {
    return Dialects.ORACLE;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Persistor<Connection, SimpleTransaction<Void>> persistor() {
    return (Persistor) persistor;
  }

  @Override
  protected SimpleTransactionManager txManager() {
    return txManager;
  }
}
