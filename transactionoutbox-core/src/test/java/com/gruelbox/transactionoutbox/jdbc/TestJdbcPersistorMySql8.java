package com.gruelbox.transactionoutbox.jdbc;

import com.gruelbox.transactionoutbox.AbstractSqlPersistor;
import com.gruelbox.transactionoutbox.AbstractSqlPersistorTest;
import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.Persistor;
import java.sql.Connection;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
class TestJdbcPersistorMySql8
    extends AbstractSqlPersistorTest<Connection, SimpleTransaction<Void>> {

  @Container
  @SuppressWarnings("rawtypes")
  private static final JdbcDatabaseContainer container =
      new MySQLContainer<>("mysql:8").withStartupTimeout(Duration.ofHours(1));

  private JdbcPersistor persistor = JdbcPersistor.builder().dialect(Dialect.MY_SQL_8).build();
  private SimpleTransactionManager txManager =
      SimpleTransactionManager.fromConnectionDetails(
          "com.mysql.cj.jdbc.Driver",
          container.getJdbcUrl(),
          container.getUsername(),
          container.getPassword());

  @Override
  protected Dialect dialect() {
    return Dialect.H2;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected AbstractSqlPersistor<Connection, SimpleTransaction<Void>> persistor() {
    Persistor result = persistor;
    return (AbstractSqlPersistor<Connection, SimpleTransaction<Void>>) result;
  }

  @Override
  protected SimpleTransactionManager txManager() {
    return txManager;
  }
}
