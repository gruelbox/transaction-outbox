package com.gruelbox.transactionoutbox;

import static com.gruelbox.transactionoutbox.Utils.toBlockingFuture;
import static com.gruelbox.transactionoutbox.Utils.uncheck;
import static com.gruelbox.transactionoutbox.Utils.uncheckedly;

import com.gruelbox.transactionoutbox.jdbc.JdbcTransactionManager;
import com.gruelbox.transactionoutbox.jdbc.SimpleTransactionManager;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import javax.sql.DataSource;
import lombok.SneakyThrows;

/**
 * @deprecated Use {@link com.gruelbox.transactionoutbox.jdbc.JdbcTransactionManager} for equivalent
 *     functionality.
 */
@Deprecated
public interface TransactionManager extends JdbcTransactionManager<Transaction> {

  /**
   * Creates a simple transaction manager which uses the specified {@link DataSource} to source
   * connections. A new connection is requested for each transaction.
   *
   * <p>Transactions will be solely controlled through {@link TransactionManager}, so this may be
   * suitable for new applications with no other transaction management. Otherwise, a custom {@link
   * TransactionManager} implementation should be used.
   *
   * @param dataSource The data source.
   * @return The transaction manager.
   */
  static ThreadLocalContextTransactionManager fromDataSource(DataSource dataSource) {
    return new JdbcShimTransactionManager(SimpleTransactionManager.fromDataSource(dataSource));
  }

  /**
   * Creates a simple transaction manager which uses the specified connection details to request a
   * new connection from the {@link java.sql.DriverManager} every time a new transaction starts.
   *
   * <p>Transactions will be solely controlled through {@link TransactionManager}, and without
   * pooling, performance will be poor. Generally, {@link #fromDataSource(DataSource)} using a
   * pooling {@code DataSource} such as that provided by Hikari is preferred.
   *
   * @param driverClass The driver class name (e.g. {@code com.mysql.cj.jdbc.Driver}).
   * @param url The JDBC url.
   * @param username The username.
   * @param password The password.
   * @return The transaction manager.
   */
  static ThreadLocalContextTransactionManager fromConnectionDetails(
      String driverClass, String url, String username, String password) {
    return new JdbcShimTransactionManager(
        SimpleTransactionManager.fromConnectionDetails(driverClass, url, username, password));
  }

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
  default void inTransaction(TransactionalWork<Transaction> work) {
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
  default <T> T inTransactionReturns(TransactionalSupplier<T, Transaction> supplier) {
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
  default <E extends Exception> void inTransactionThrows(
      ThrowingTransactionalWork<E, Transaction> work) throws E {
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
      ThrowingTransactionalSupplier<T, E, Transaction> work) throws E;

  @Override
  default <T> CompletableFuture<T> transactionally(
      Function<Transaction, CompletableFuture<T>> work) {
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
