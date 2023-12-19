package com.gruelbox.transactionoutbox.testing;

import com.gruelbox.transactionoutbox.ThrowingRunnable;
import com.gruelbox.transactionoutbox.TransactionManager;
import com.gruelbox.transactionoutbox.UncheckedException;
import java.sql.Statement;

public class TestUtils {

  @SuppressWarnings("SameParameterValue")
  public static void runSql(TransactionManager transactionManager, String sql) {
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

  public static void uncheck(ThrowingRunnable runnable) {
    try {
      runnable.run();
    } catch (Exception e) {
      uncheckAndThrow(e);
    }
  }

  public static <T> T uncheckAndThrow(Throwable e) {
    if (e instanceof RuntimeException) {
      throw (RuntimeException) e;
    }
    if (e instanceof Error) {
      throw (Error) e;
    }
    throw new UncheckedException(e);
  }
}
