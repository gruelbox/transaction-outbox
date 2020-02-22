package com.gruelbox.transactionoutbox;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * A simple {@link TransactionManager} implementation suitable for applications with no existing
 * transaction management.
 */
@SuperBuilder
@Slf4j
final class SimpleTransactionManager extends AbstractThreadLocalTransactionManager {

  private final ThreadLocal<ArrayList<Runnable>> postCommitHooks = new ThreadLocal<>();
  private final ThreadLocal<Map<String, PreparedStatement>> statements = new ThreadLocal<>();

  @Override
  public <T> T inTransactionReturnsThrows(Callable<T> callable) throws Exception {
    return withConnectionReturnsThrows(
        () -> {
          Connection conn = getActiveConnection();
          if (conn.getAutoCommit()) {
            log.debug("Setting auto-commit false");
            Utils.uncheck(() -> conn.setAutoCommit(false));
          }
          try {
            T result = processAndCommitOrRollback(callable, conn);
            if (postCommitHooks.get() != null) {
              log.debug("Running post-commit hooks");
              postCommitHooks.get().forEach(Runnable::run);
            }
            return result;
          } finally {
            if (statements.get() != null) {
              log.debug("Closing batch statements");
              Utils.safelyClose(statements.get().values());
              statements.remove();
            }
            postCommitHooks.remove();
          }
        });
  }

  private <T> T processAndCommitOrRollback(Callable<T> callable, Connection conn) throws Exception {
    try {
      log.debug("Processing work");
      T result = callable.call();
      if (statements.get() != null) {
        log.debug("Flushing batches");
        statements.get().values().forEach(it -> Utils.uncheck(it::executeBatch));
      }
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
  public void addPostCommitHook(Runnable runnable) {
    if (postCommitHooks.get() == null) postCommitHooks.set(new ArrayList<>());
    postCommitHooks.get().add(runnable);
  }

  @Override
  public PreparedStatement prepareBatchStatement(String sql) {
    if (statements.get() == null) statements.set(new HashMap<>());
    return statements
        .get()
        .computeIfAbsent(
            sql, s -> Utils.uncheckedly(() -> getActiveConnection().prepareStatement(s)));
  }
}
