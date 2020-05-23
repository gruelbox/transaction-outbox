package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.jdbc.JdbcPersistor;
import com.gruelbox.transactionoutbox.jdbc.JdbcPersistor.JdbcPersistorBuilder;

@Deprecated
public class DefaultPersistor {

  public static JdbcPersistor forDialect(Dialect dialect) {
    return JdbcPersistor.forDialect((com.gruelbox.transactionoutbox.sql.Dialect) dialect);
  }

  public static JdbcPersistorBuilder builder() {
    return JdbcPersistor.builder();
  }
}
