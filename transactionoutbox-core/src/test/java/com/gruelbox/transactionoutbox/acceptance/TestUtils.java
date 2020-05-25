package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.jdbc.JdbcTransactionManager;
import java.sql.Statement;

class TestUtils {

  @SuppressWarnings("SameParameterValue")
  static void runSqlViaJdbc(JdbcTransactionManager<?, ?> transactionManager, String sql) {
    transactionManager.inTransaction(
        tx -> {
          try {
            try (Statement statement = tx.connection().createStatement()) {
              statement.execute(sql);
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }
}
