package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.AbstractThreadLocalTransactionManager.ThreadLocalTransaction;
import java.sql.Connection;
import java.sql.SQLException;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * A simple {@link TransactionManager} implementation suitable for applications with no existing
 * transaction management.
 */
@SuperBuilder
@Slf4j
final class SimpleTransactionManager
    extends AbstractThreadLocalTransactionManager<ThreadLocalTransaction> {

  private final ConnectionProvider connectionProvider;

  @Override
  public <T, E extends Exception> T inTransactionReturnsThrows(
      ThrowingTransactionalSupplier<T, E> work) throws E {
    return withTransaction(
        atx -> {
          T result = processAndCommitOrRollback(work, (ThreadLocalTransaction) atx);
          ((ThreadLocalTransaction) atx).processHooks();
          return result;
        });
  }

  private <T, E extends Exception> T processAndCommitOrRollback(
      ThrowingTransactionalSupplier<T, E> work, ThreadLocalTransaction transaction) throws E {
    try {
      log.debug("Processing work");
      T result = work.doWork(transaction);
      transaction.flushBatches();
      log.debug("Committing transaction");
      transaction.commit();
      return result;
    } catch (Exception e) {
      try {
        log.warn(
            "Exception in transactional block ({}{}). Rolling back. See later messages for detail",
            e.getClass().getSimpleName(),
            e.getMessage() == null ? "" : (" - " + e.getMessage()));
        transaction.rollback();
      } catch (Exception ex) {
        log.warn("Failed to roll back", ex);
      }
      throw e;
    }
  }

  private <T, E extends Exception> T withTransaction(ThrowingTransactionalSupplier<T, E> work)
      throws E {
    try (Connection connection = connectionProvider.obtainConnection();
        ThreadLocalTransaction transaction =
            pushTransaction(new ThreadLocalTransaction(connection))) {
      log.debug("Got connection {}", connection);
      boolean autoCommit = transaction.connection().getAutoCommit();
      if (autoCommit) {
        log.debug("Setting auto-commit false");
        Utils.uncheck(() -> transaction.connection().setAutoCommit(false));
      }
      try {
        return work.doWork(transaction);
      } finally {
        connection.setAutoCommit(autoCommit);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    } finally {
      popTransaction();
    }
  }
}
