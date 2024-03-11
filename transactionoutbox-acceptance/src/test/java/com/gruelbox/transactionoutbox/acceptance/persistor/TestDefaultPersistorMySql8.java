package com.gruelbox.transactionoutbox.acceptance.persistor;

import com.gruelbox.transactionoutbox.DefaultPersistor;
import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.TransactionManager;
import com.gruelbox.transactionoutbox.testing.AbstractPersistorTest;

class TestDefaultPersistorMySql8 extends AbstractPersistorTest {

  private final DefaultPersistor persistor =
      DefaultPersistor.builder().dialect(Dialect.MY_SQL_8).build();
  private final TransactionManager txManager =
      TransactionManager.fromConnectionDetails(
          "com.mysql.cj.jdbc.Driver",
          "jdbc:tc:mysql:8:///test?TC_REUSABLE=true&TC_TMPFS=/var/lib/mysql:rw",
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

  @Override
  protected Dialect dialect() {
    return Dialect.MY_SQL_8;
  }
}
