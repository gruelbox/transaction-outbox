package com.gruelbox.transactionoutbox.jdbc;

import com.gruelbox.transactionoutbox.Beta;
import com.gruelbox.transactionoutbox.Invocation;
import com.gruelbox.transactionoutbox.NoTransactionActiveException;
import com.gruelbox.transactionoutbox.NotApi;
import com.gruelbox.transactionoutbox.TransactionalInvocation;
import com.gruelbox.transactionoutbox.spi.ThrowingTransactionalSupplier;
import com.gruelbox.transactionoutbox.spi.ThrowingTransactionalWork;
import com.gruelbox.transactionoutbox.spi.TransactionManagerSupport;
import com.gruelbox.transactionoutbox.spi.TransactionalSupplier;
import com.gruelbox.transactionoutbox.spi.TransactionalWork;
import java.lang.reflect.Method;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Beta
@NotApi
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class AbstractThreadLocalJdbcTransactionManager<TX extends JdbcTransaction>
    implements JdbcTransactionManager<TX> {

  private final ThreadLocal<Deque<TX>> transactions = ThreadLocal.withInitial(LinkedList::new);

  @Override
  public final void inTransaction(Runnable runnable) {
    JdbcTransactionManager.super.inTransaction(runnable);
  }

  @Override
  public final void inTransaction(TransactionalWork<TX> work) {
    JdbcTransactionManager.super.inTransaction(work);
  }

  @Override
  public final <T> T inTransactionReturns(TransactionalSupplier<T, TX> supplier) {
    return JdbcTransactionManager.super.inTransactionReturns(supplier);
  }

  @Override
  public final <E extends Exception> void inTransactionThrows(ThrowingTransactionalWork<E, TX> work)
      throws E {
    JdbcTransactionManager.super.inTransactionThrows(work);
  }

  @Override
  public final <T> CompletableFuture<T> transactionally(Function<TX, CompletableFuture<T>> work) {
    return JdbcTransactionManager.super.transactionally(work);
  }

  @Override
  public TransactionalInvocation extractTransaction(Method method, Object[] args) {
    return requireTransactionReturns(
        transaction ->
            TransactionManagerSupport.toTransactionalInvocation(method, args, transaction));
  }

  @Override
  public Invocation injectTransaction(Invocation invocation, TX transaction) {
    return invocation;
  }

  /**
   * Should do any work necessary to start a (new) transaction, call {@code work} and then either
   * commit on success or rollback on failure, flushing and closing any prepared statements prior to
   * a commit and firing post commit hooks immediately afterwards.
   *
   * @param <T> The type returned.
   * @param work Code which must be called while the transaction is active.
   * @param <E> The exception type.
   * @return The result of {@code supplier}.
   * @throws E If any exception is thrown by {@link Runnable}.
   */
  public abstract <T, E extends Exception> T inTransactionReturnsThrows(
      ThrowingTransactionalSupplier<T, E, TX> work) throws E;

  /**
   * Runs the specified work in the context of the "current" transaction (the definition of which is
   * up to the implementation).
   *
   * @param work Code which must be called while the transaction is active.
   * @param <E> The exception type.
   * @throws E If any exception is thrown by {@link Runnable}.
   * @throws NoTransactionActiveException If a transaction is not currently active.
   */
  public final <E extends Exception> void requireTransaction(ThrowingTransactionalWork<E, TX> work)
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
  public final <T, E extends Exception> T requireTransactionReturns(
      ThrowingTransactionalSupplier<T, E, TX> work) throws E, NoTransactionActiveException {
    return work.doWork(peekTransaction().orElseThrow(NoTransactionActiveException::new));
  }

  public final TX pushTransaction(TX transaction) {
    transactions.get().push(transaction);
    return transaction;
  }

  public final TX popTransaction() {
    TX result = transactions.get().pop();
    if (transactions.get().isEmpty()) {
      transactions.remove();
    }
    return result;
  }

  protected Optional<TX> peekTransaction() {
    return Optional.ofNullable(transactions.get().peek());
  }
}
