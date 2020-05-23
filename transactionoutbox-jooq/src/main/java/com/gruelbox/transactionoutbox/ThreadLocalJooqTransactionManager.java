package com.gruelbox.transactionoutbox;

import org.jooq.DSLContext;

/**
 * jOOQ transaction manager which uses thread-local context. Best used with {@link
 * org.jooq.impl.ThreadLocalTransactionProvider}. Relies on a {@link JooqTransactionListener} being
 * attached to the {@link DSLContext}.
 */
public interface ThreadLocalJooqTransactionManager extends JooqTransactionManager {

  /**
   * Runs the specified work in the context of the "current" transaction (the definition of which is
   * up to the implementation).
   *
   * @param work Code which must be called while the transaction is active.
   * @param <E> The exception type.
   * @throws E If any exception is thrown by {@link Runnable}.
   * @throws NoTransactionActiveException If a transaction is not currently active.
   */
  default <E extends Exception> void requireTransaction(
      ThrowingTransactionalWork<E, JooqTransaction> work) throws E, NoTransactionActiveException {
    requireTransactionReturns(ThrowingTransactionalSupplier.fromWork(work));
  }

  /**
   * Runs the specified work in the context of the "current" transaction (the definition of which is
   * up to the implementation).
   *
   * @param work Code which must be called while the transaction is active.
   * @param <T> The type returned.
   * @param <E> The exception type.
   * @return The value returned by {@code work}.
   * @throws E If any exception is thrown by {@link Runnable}.
   * @throws NoTransactionActiveException If a transaction is not currently active.
   * @throws UnsupportedOperationException If the transaction manager does not support thread-local
   *     context.
   */
  <T, E extends Exception> T requireTransactionReturns(
      ThrowingTransactionalSupplier<T, E, JooqTransaction> work)
      throws E, NoTransactionActiveException;
}
