package com.gruelbox.transactionoutbox.spi;

import com.gruelbox.transactionoutbox.Persistor;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Access and manipulation of a currently-active transaction. This is an extremely high-level
 * generalisation; it is advised that you refer to subtypes.
 *
 * @param <CN> The type which the associated {@link Persistor} implementation will use to interact
 *     with the data store.
 */
public interface BaseTransaction<CN> {

  /**
   * @return The object used by the associated {@link Persistor} to interact with the data store.
   *     This is usually low-level, such as JDBC's {@code Connection}, rather than a higher-level
   *     API, and as such will only ever consist of a small handful of types.
   */
  CN connection();

  /**
   * @param <T> The context type. Coerced on read.
   * @return A {@link BaseTransactionManager}-specific object representing the context of this
   *     transaction. This is usually a <em>higher</em> level API than {@link #connection()} and
   *     will be specific to the transaction manager and likely the data access API being used.
   */
  default <T> T context() {
    return null;
  }

  /**
   * Will be called to perform work immediately after the current transaction is committed. Expects
   * a {@link CompletableFuture} which will be called in a non-blocking fashion. For blocking data
   * APIs, subtypes of {@link BaseTransaction} should introduce more blocking- friendly overloads of
   * this method.
   *
   * @param hook The code to run post-commit.
   */
  void addPostCommitHook(Supplier<CompletableFuture<Void>> hook);
}
