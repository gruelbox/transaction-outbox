package com.gruelbox.transactionoutbox;

import java.sql.Connection;
import java.util.Optional;
import java.util.concurrent.Callable;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuperBuilder
abstract class AbstractThreadLocalTransactionManager implements TransactionManager {

  private final ConnectionProvider connectionProvider;
  private final ThreadLocal<Connection> activeConnection = new ThreadLocal<>();

  @Override
  public Connection getActiveConnection() {
    return Optional.ofNullable(activeConnection.get())
        .orElseThrow(NoTransactionActiveException::new);
  }

  @SuppressWarnings({"SameReturnValue", "unused"})
  void withConnectionThrows(ThrowingRunnable runnable) throws Exception {
    withConnectionReturnsThrows(
        () -> {
          runnable.run();
          return null;
        });
  }

  <T> T withConnectionReturnsThrows(Callable<T> callable) throws Exception {
    if (activeConnection.get() != null) {
      throw new TransactionAlreadyActiveException();
    }
    try (Connection conn = connectionProvider.obtainConnection()) {
      log.debug("Got connection {}", conn);
      activeConnection.set(conn);
      return callable.call();
    } finally {
      activeConnection.remove();
    }
  }
}
