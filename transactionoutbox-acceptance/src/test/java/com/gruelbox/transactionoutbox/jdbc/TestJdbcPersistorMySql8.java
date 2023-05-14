package com.gruelbox.transactionoutbox.jdbc;

import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.sql.AbstractPersistorTest;
import com.gruelbox.transactionoutbox.sql.Dialect;
import com.gruelbox.transactionoutbox.sql.Dialects;
import java.sql.Connection;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
class TestJdbcPersistorMySql8 extends AbstractPersistorTest<Connection, SimpleTransaction<Void>> {

  @Container
  @SuppressWarnings({"rawtypes", "resource"})
  private static final JdbcDatabaseContainer container =
      new MySQLContainer<>("mysql:8").withStartupTimeout(Duration.ofHours(1));

  private final JdbcPersistor persistor =
      JdbcPersistor.builder().dialect(Dialects.MY_SQL_8).build();
  private final SimpleTransactionManager txManager =
      SimpleTransactionManager.fromConnectionDetails(
          "com.mysql.cj.jdbc.Driver",
          container.getJdbcUrl(),
          container.getUsername(),
          container.getPassword());

  @Override
  protected Dialect dialect() {
    return Dialects.MY_SQL_8;
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
