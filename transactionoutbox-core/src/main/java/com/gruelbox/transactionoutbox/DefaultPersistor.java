package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.jdbc.JdbcPersistor;
import com.gruelbox.transactionoutbox.jdbc.JdbcPersistor.JdbcPersistorBuilder;

/**
 * @deprecated Use {@link JdbcPersistor} or {@code R2dbcPersistor}.
 */
@Deprecated
public class DefaultPersistor {

  public static JdbcPersistor forDialect(Dialect dialect) {
    return JdbcPersistor.forDialect(dialect);
  }

  public static JdbcPersistorBuilder builder() {
    return JdbcPersistor.builder();
  }
}
