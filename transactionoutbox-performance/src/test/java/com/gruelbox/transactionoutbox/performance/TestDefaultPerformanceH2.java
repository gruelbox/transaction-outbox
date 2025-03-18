package com.gruelbox.transactionoutbox.performance;

import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.TransactionManager;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class TestDefaultPerformanceH2 extends AbstractPerformanceTest {
  private final TransactionManager txManager =
      TransactionManager.fromConnectionDetails(
          "org.h2.Driver",
          "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;DEFAULT_LOCK_TIMEOUT=2000;LOB_TIMEOUT=2000;MV_STORE=TRUE",
          "test",
          "test");

  @Override
  protected TransactionManager txManager() {
    return txManager;
  }

  @Override
  protected Dialect dialect() {
    return Dialect.H2;
  }
}
