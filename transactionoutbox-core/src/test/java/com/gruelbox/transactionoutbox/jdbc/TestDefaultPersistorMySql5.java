package com.gruelbox.transactionoutbox.jdbc;

import com.gruelbox.transactionoutbox.Dialect;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
class TestDefaultPersistorMySql5 extends AbstractDefaultPersistorTest {

  @Container
  @SuppressWarnings("rawtypes")
  private static final JdbcDatabaseContainer container =
      new MySQLContainer<>("mysql:5").withStartupTimeout(Duration.ofHours(1));

  private JdbcPersistor persistor = JdbcPersistor.builder().dialect(Dialect.MY_SQL_5).build();
  private SimpleTransactionManager txManager =
      SimpleTransactionManager.fromConnectionDetails(
          "com.mysql.cj.jdbc.Driver",
          container.getJdbcUrl(),
          container.getUsername(),
          container.getPassword());

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
    return Dialect.MY_SQL_5;
  }
}
