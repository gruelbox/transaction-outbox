package com.gruelbox.transactionoutbox.jdbc;

import com.gruelbox.transactionoutbox.Dialect;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
class TestJdbcPersistorH2 extends AbstractJdbcPersistorTest {

  private JdbcPersistor persistor = JdbcPersistor.builder().dialect(Dialect.H2).build();
  private SimpleTransactionManager txManager =
      SimpleTransactionManager.fromConnectionDetails(
          "org.h2.Driver",
          "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;DEFAULT_LOCK_TIMEOUT=2000;LOB_TIMEOUT=2000;MV_STORE=TRUE",
          "test",
          "test");

  @Override
  protected JdbcPersistor persistor() {
    return persistor;
  }

  @Override
  protected SimpleTransactionManager txManager() {
    return txManager;
  }

  @Override
  protected Dialect dialect() {
    return Dialect.H2;
  }
}
