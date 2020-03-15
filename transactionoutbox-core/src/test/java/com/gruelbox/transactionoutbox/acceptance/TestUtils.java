package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.TransactionManager;
import java.sql.Statement;

class TestUtils {

  @SuppressWarnings("SameParameterValue")
  static void runSql(TransactionManager transactionManager, String sql) {
    transactionManager.inTransaction(
        () -> {
          try {
            try (Statement statement = transactionManager.getActiveConnection().createStatement()) {
              statement.execute(sql);
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }
}
