package com.gruelbox.transactionoutbox;

import lombok.extern.slf4j.Slf4j;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
class TestDefaultPersistorH2 extends AbstractDefaultPersistorTest {

  private DefaultPersistor persistor = DefaultPersistor.builder().dialect(Dialect.H2).build();
  private TransactionManager txManager =
      TransactionManager.fromConnectionDetails(
          "org.h2.Driver",
          "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;DEFAULT_LOCK_TIMEOUT=2000;LOB_TIMEOUT=2000;MV_STORE=TRUE",
          "test",
          "test");

  @Override
  protected DefaultPersistor persistor() {
    return persistor;
  }

  @Override
  protected TransactionManager txManager() {
    return txManager;
  }
}
