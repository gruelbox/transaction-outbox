package com.gruelbox.transactionoutbox.jdbc;

import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.sql.AbstractPersistorTest;
import com.gruelbox.transactionoutbox.sql.Dialect;
import com.gruelbox.transactionoutbox.sql.Dialects;
import java.sql.Connection;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
class TestJdbcPersistorH2 extends AbstractPersistorTest<Connection, SimpleTransaction<Void>> {

  private final JdbcPersistor persistor = JdbcPersistor.builder().dialect(Dialects.H2).build();
  private final SimpleTransactionManager txManager =
      SimpleTransactionManager.fromConnectionDetails(
          "org.h2.Driver",
          "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;DEFAULT_LOCK_TIMEOUT=2000;LOB_TIMEOUT=2000;MV_STORE=TRUE",
          "test",
          "test");

  @Override
  protected Dialect dialect() {
    return Dialects.H2;
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
