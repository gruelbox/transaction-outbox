package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.spi.TransactionManagerSupport;
import java.lang.reflect.Method;

/**
 * @deprecated If implementing a thread-local transaction manager, if you want or need the {@link
 *     #requireTransaction(ThrowingTransactionalWork)}, simply extend {@link
 *     com.gruelbox.transactionoutbox.jdbc.JdbcTransactionManager} and include these methods
 *     directly in your implementation rather than inheriting from this interface. The {@link
 *     #extractTransaction(Method, Object[])} and {@link #injectTransaction(Invocation,
 *     Transaction)} methods can then be implemented as below. If you don't need those methods,
 *     simply implement {@link #extractTransaction(Method, Object[])} to pull the transaction from
 *     your thread local context directly.
 */
@Deprecated
public interface ThreadLocalContextTransactionManager extends TransactionManager {

  <T, E extends Exception> T inTransactionReturnsThrows(ThrowingTransactionalSupplier<T, E> work)
      throws E;

  @Override
  default <T, E extends Exception> T inTransactionReturnsThrows(
      com.gruelbox.transactionoutbox.spi.ThrowingTransactionalSupplier<T, E, Transaction> work)
      throws E {
    return inTransactionReturnsThrows(work::doWork);
  }

  /**
   * Runs the specified work in the context of the "current" transaction (the definition of which is
   * up to the implementation).
   *
   * @param work Code which must be called while the transaction is active.
   * @param <E> The exception type.
   * @throws E If any exception is thrown by {@link Runnable}.
   * @throws NoTransactionActiveException If a transaction is not currently active.
   */
  default <E extends Exception> void requireTransaction(ThrowingTransactionalWork<E> work)
      throws E, NoTransactionActiveException {
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
  <T, E extends Exception> T requireTransactionReturns(ThrowingTransactionalSupplier<T, E> work)
      throws E, NoTransactionActiveException;

  @Override
  default TransactionalInvocation extractTransaction(Method method, Object[] args) {
    return requireTransactionReturns(
        transaction ->
            TransactionManagerSupport.toTransactionalInvocation(method, args, transaction));
  }

  @Override
  default Invocation injectTransaction(Invocation invocation, Transaction transaction) {
    return invocation;
  }
}
