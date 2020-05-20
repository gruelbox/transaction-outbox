package com.gruelbox.transactionoutbox;

import java.sql.Connection;
import java.sql.PreparedStatement;

/** Access and manipulation of a currently-active transaction. */
public interface Transaction {

  /** @return The connection for the transaction. */
  Connection connection();

  /**
   * @return A {@link TransactionManager}-specific object representing the context of this
   *     transaction. Intended for use with {@link TransactionManager} implementations that support
   *     explicitly-passed transaction context injection into method arguments.
   */
  default <T> T context() {
    return null;
  }

  /**
   * Creates a prepared statement which will be cached and re-used within a transaction. Any batch
   * on these statements is executed before the transaction is committed, and automatically closed.
   *
   * @param sql The SQL statement
   * @return The statement.
   */
  PreparedStatement prepareBatchStatement(String sql);

  /**
   * Will be called to perform work immediately after the current transaction is committed. This
   * should occur in the same thread and will generally not be long-lasting.
   *
   * @param runnable The code to run post-commit.
   */
  void addPostCommitHook(Runnable runnable);
}
