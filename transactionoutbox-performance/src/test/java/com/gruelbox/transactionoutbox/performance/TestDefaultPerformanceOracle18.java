package com.gruelbox.transactionoutbox.performance;

import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.TransactionManager;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class TestDefaultPerformanceOracle18 extends AbstractPerformanceTest {

  @SuppressWarnings({"rawtypes"})
  private static final JdbcDatabaseContainer container = ContainerUtils.getOracle18Container();

  private final TransactionManager txManager =
      TransactionManager.fromConnectionDetails(
          "com.mysql.cj.jdbc.Driver",
          container.getJdbcUrl(),
          container.getUsername(),
          container.getPassword());

  @Override
  protected TransactionManager txManager() {
    return txManager;
  }

  @Override
  protected Dialect dialect() {
    return Dialect.ORACLE;
  }

  @BeforeAll
  public static void beforeAll() {
    container.start();
  }
}
