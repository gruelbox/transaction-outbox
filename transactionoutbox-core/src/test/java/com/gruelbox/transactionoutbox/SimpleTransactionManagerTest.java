package com.gruelbox.transactionoutbox;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLException;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;

class SimpleTransactionManagerTest {

  @Test
  void connectionFailureIsNotMaskedByPop() {
    SQLException connectionFailure = new SQLException("boom");
    ConnectionProvider failingProvider =
        () -> {
          throw new UncheckedException(connectionFailure);
        };

    SimpleTransactionManager transactionManager =
        SimpleTransactionManager.builder().connectionProvider(failingProvider).build();

    RuntimeException thrown =
        assertThrows(RuntimeException.class, () -> transactionManager.inTransaction(() -> {}));

    assertFalse(
        containsCause(thrown, NoSuchElementException.class),
        "Real connection failure was masked by an empty-stack pop");
    assertSame(connectionFailure, rootCause(thrown), "Original SQLException should be preserved");
  }

  private static boolean containsCause(Throwable throwable, Class<? extends Throwable> type) {
    for (Throwable t = throwable; t != null; t = t.getCause()) {
      if (type.isInstance(t)) {
        return true;
      }
    }
    return false;
  }

  private static Throwable rootCause(Throwable throwable) {
    Throwable t = throwable;
    while (t.getCause() != null && t.getCause() != t) {
      t = t.getCause();
    }
    return t;
  }
}
