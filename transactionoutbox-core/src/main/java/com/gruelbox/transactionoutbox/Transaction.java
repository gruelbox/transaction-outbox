package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.jdbc.JdbcTransaction;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Represents a transaction in JDBC-land.
 *
 * @deprecated Use {@link com.gruelbox.transactionoutbox.jdbc.JdbcTransaction} for equivalent
 *     functionality.
 */
@Deprecated
public interface Transaction extends JdbcTransaction {

  @Override
  default void addPostCommitHook(Runnable hook) {
    throw new UnsupportedOperationException("This method needs to be implemented");
  }

  @Override
  default void addPostCommitHook(Supplier<CompletableFuture<Void>> hook) {
    addPostCommitHook(() -> Utils.join(hook.get()));
  }
}
