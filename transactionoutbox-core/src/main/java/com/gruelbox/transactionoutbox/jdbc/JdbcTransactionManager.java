package com.gruelbox.transactionoutbox.jdbc;

import static com.gruelbox.transactionoutbox.Utils.toBlockingFuture;
import static com.gruelbox.transactionoutbox.Utils.uncheck;
import static com.gruelbox.transactionoutbox.Utils.uncheckedly;

import com.gruelbox.transactionoutbox.ThrowingTransactionalSupplier;
import com.gruelbox.transactionoutbox.ThrowingTransactionalWork;
import com.gruelbox.transactionoutbox.TransactionalSupplier;
import com.gruelbox.transactionoutbox.TransactionalWork;
import com.gruelbox.transactionoutbox.spi.BaseTransactionManager;
import java.sql.Connection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import lombok.SneakyThrows;

/**
 * Specialises {@link BaseTransactionManager} for use with JDBC. Since JDBC is a fundamentally
 * blocking API, this type introduces a number of useful blocking APIs which allow client-side code
 * to interact with the transaction manager in a more natural way.
 *
 * @param <TX> THe transaction type
 */
public interface JdbcTransactionManager<TX extends JdbcTransaction>
    extends BaseTransactionManager<Connection, TX> {

  /**
   * Should do any work necessary to start a (new) transaction, call {@code runnable} and then
   * either commit on success or rollback on failure, flushing and closing any prepared statements
   * prior to a commit and firing post commit hooks immediately afterwards
   *
   * @param runnable Code which must be called while the transaction is active..
   */
  default void inTransaction(Runnable runnable) {
    uncheck(() -> inTransactionReturnsThrows(ThrowingTransactionalSupplier.fromRunnable(runnable)));
  }

  /**
   * Should do any work necessary to start a (new) transaction, call {@code runnable} and then
   * either commit on success or rollback on failure, flushing and closing any prepared statements
   * prior to a commit and firing post commit hooks immediately afterwards
   *
   * @param work Code which must be called while the transaction is active..
   */
  default void inTransaction(TransactionalWork<TX> work) {
    uncheck(() -> inTransactionReturnsThrows(ThrowingTransactionalSupplier.fromWork(work)));
  }

  /**
   * Should do any work necessary to start a (new) transaction, call {@code runnable} and then
   * either commit on success or rollback on failure, flushing and closing any prepared statements
   * prior to a commit and firing post commit hooks immediately afterwards.
   *
   * @param <T> The type returned.
   * @param supplier Code which must be called while the transaction is active.
   * @return The result of {@code supplier}.
   */
  default <T> T inTransactionReturns(TransactionalSupplier<T, TX> supplier) {
    return uncheckedly(
        () -> inTransactionReturnsThrows(ThrowingTransactionalSupplier.fromSupplier(supplier)));
  }

  /**
   * Should do any work necessary to start a (new) transaction, call {@code runnable} and then
   * either commit on success or rollback on failure, flushing and closing any prepared statements
   * prior to a commit and firing post commit hooks immediately afterwards.
   *
   * @param work Code which must be called while the transaction is active.
   * @param <E> The exception type.
   * @throws E If any exception is thrown by {@link Runnable}.
   */
  @SuppressWarnings("SameReturnValue")
  default <E extends Exception> void inTransactionThrows(ThrowingTransactionalWork<E, TX> work)
      throws E {
    inTransactionReturnsThrows(ThrowingTransactionalSupplier.fromWork(work));
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
  <T, E extends Exception> T inTransactionReturnsThrows(
      ThrowingTransactionalSupplier<T, E, TX> work) throws E;

  @Override
  default <T> CompletableFuture<T> transactionally(Function<TX, CompletableFuture<T>> work) {
    return toBlockingFuture(
        () ->
            inTransactionReturnsThrows(
                tx -> {
                  try {
                    return work.apply(tx).join();
                  } catch (CompletionException e) {
                    sneakyThrow(e.getCause());
                    return null;
                  }
                }));
  }

  @SneakyThrows
  private void sneakyThrow(Throwable t) {
    throw t;
  }
}
