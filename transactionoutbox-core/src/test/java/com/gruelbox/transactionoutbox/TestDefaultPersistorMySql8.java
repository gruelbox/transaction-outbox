package com.gruelbox.transactionoutbox;

import java.time.Duration;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class TestDefaultPersistorMySql8 extends AbstractDefaultPersistorTest {

  @Container
  @SuppressWarnings("rawtypes")
  private static final JdbcDatabaseContainer container =
      new MySQLContainer<>("mysql:8").withStartupTimeout(Duration.ofHours(1));

  private final DefaultPersistor persistor = DefaultPersistor.builder().dialect(dialect()).build();
  private TransactionManager txManager =
      TransactionManager.fromConnectionDetails(
          "com.mysql.cj.jdbc.Driver",
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
  protected DialectSql dialect() {
    return new DialectSqlMySQL8Impl();
  }
}
