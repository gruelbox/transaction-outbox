package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.jdbc.JdbcPersistor;
import com.gruelbox.transactionoutbox.jdbc.JdbcTransaction;
import com.gruelbox.transactionoutbox.sql.Dialect;
import java.sql.Connection;

public interface Persistor
    extends com.gruelbox.transactionoutbox.spi.Persistor<Connection, JdbcTransaction<?>> {

  static Persistor forDialect(Dialect dialect) {
    return JdbcPersistor.forDialect(dialect);
  }

  static JdbcPersistor.JdbcPersistorBuilder builder() {
    return JdbcPersistor.builder();
  }
}
