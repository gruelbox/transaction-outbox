package com.gruelbox.transactionoutbox.jdbc;

import com.gruelbox.transactionoutbox.Utils;
import com.gruelbox.transactionoutbox.spi.Transaction;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.function.Supplier;

/**
 * Represents a transaction in JDBC-land.
 *
 * @param <CX> The type that the client code uses to interact with the transaction.
 */
public interface JdbcTransaction<CX> extends Transaction<Connection, CX> {

  /**
   * Creates a JDBC prepared statement which will be cached and re-used within a transaction. Any
   * batch on these statements is executed before the transaction is committed, and automatically
   * closed.
   *
   * @param sql The SQL statement
   * @return The statement.
   */
  PreparedStatement prepareBatchStatement(String sql);

  /**
   * Blocking implementation of {@link #addPostCommitHook(Supplier)}, allowing idiomatic use in
   * blocking client code.
   *
   * @param hook The code to run post-commit.
   */
  default void addPostCommitHook(Runnable hook) {
    this.addPostCommitHook(() -> Utils.toBlockingFuture(hook::run));
  }
}
