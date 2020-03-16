package com.gruelbox.transactionoutbox;

import java.sql.Connection;
import java.util.Optional;
import java.util.concurrent.Callable;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * A simple {@link TransactionManager} implementation suitable for applications with no existing
 * transaction management.
 */
@SuperBuilder
@Slf4j
final class SimpleTransactionManager extends BaseTransactionManager {

  private final ConnectionProvider connectionProvider;

  @Override
  public <T> T inTransactionReturnsThrows(Callable<T> callable) throws Exception {
    return withConnection(
        () -> {
          Connection conn = peekConnection();
          if (conn.getAutoCommit()) {
            log.debug("Setting auto-commit false");
            Utils.uncheck(() -> conn.setAutoCommit(false));
          }
          pushLists();
          try {
            T result = processAndCommitOrRollback(callable, conn);
            processHooks();
            return result;
          } finally {
            closeBatchStatements();
            popLists();
          }
        });
  }

  private <T> T processAndCommitOrRollback(Callable<T> callable, Connection conn) throws Exception {
    try {
      log.debug("Processing work");
      T result = callable.call();
      flushBatches();
      log.debug("Committing transaction");
      conn.commit();
      return result;
    } catch (Exception e) {
      try {
        log.warn(
            "Exception in transactional block ({}{}). Rolling back. See later messages for detail",
            e.getClass().getSimpleName(),
            e.getMessage() == null ? "" : (" - " + e.getMessage()));
        conn.rollback();
      } catch (Exception ex) {
        log.warn("Failed to roll back", ex);
      }
      throw e;
    }
  }

  @Override
  public Connection getActiveConnection() {
    return Optional.ofNullable(peekConnection())
        .orElseThrow(NoTransactionActiveException::new);
  }

  <T> T withConnection(Callable<T> callable) throws Exception {
    try (Connection conn = connectionProvider.obtainConnection()) {
      log.debug("Got connection {}", conn);
      pushConnection(conn);
      try {
        return callable.call();
      } finally {
        popConnection();
      }
    }
  }
}
