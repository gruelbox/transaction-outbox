package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.ThrowingRunnable;
import com.gruelbox.transactionoutbox.TransactionManager;
import com.gruelbox.transactionoutbox.UncheckedException;
import java.sql.Statement;

class TestUtils {

  @SuppressWarnings("SameParameterValue")
  static void runSql(TransactionManager transactionManager, String sql) {
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

  static void uncheck(ThrowingRunnable runnable) {
    try {
      runnable.run();
    } catch (Exception e) {
      uncheckAndThrow(e);
    }
  }

  static <T> T uncheckAndThrow(Throwable e) {
    if (e instanceof RuntimeException) {
      throw (RuntimeException) e;
    }
    if (e instanceof Error) {
      throw (Error) e;
    }
    throw new UncheckedException(e);
  }
}
