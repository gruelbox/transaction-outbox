package com.gruelbox.transactionoutbox;

import static com.gruelbox.transactionoutbox.Utils.uncheck;
import static com.gruelbox.transactionoutbox.Utils.uncheckedly;

import javax.sql.DataSource;

/**
 * Key interface giving {@link TransactionOutbox} access to JDBC. In most applications with existing
 * transaction management, this will be a custom implementation. However, {@link
 * SimpleTransactionManager} is provided as a simplistic example for small, standalone applications.
 */
public interface TransactionManager {

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
  static TransactionManager fromDataSource(DataSource dataSource) {
    return SimpleTransactionManager.builder()
        .connectionProvider(DataSourceConnectionProvider.builder().dataSource(dataSource).build())
        .build();
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
  static TransactionManager fromConnectionDetails(
      String driverClass, String url, String username, String password) {
    return SimpleTransactionManager.builder()
        .connectionProvider(
            DriverConnectionProvider.builder()
                .driverClassName(driverClass)
                .url(url)
                .user(username)
                .password(password)
                .build())
        .build();
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
  default void inTransaction(TransactionalWork work) {
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
  default <T> T inTransactionReturns(TransactionalSupplier<T> supplier) {
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
  default <E extends Exception> void inTransactionThrows(ThrowingTransactionalWork<E> work)
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
  <T, E extends Exception> T inTransactionReturnsThrows(ThrowingTransactionalSupplier<T, E> work)
      throws E;

  /**
   * Runs the specified work in the context of the "current" transaction (the definition of which is
   * up to the implementation).
   *
   * @param work Code which must be called while the transaction is active.
   * @param <E> The exception type.
   * @throws E If any exception is thrown by {@link Runnable}.
   * @throws NoTransactionActiveException If a transaction is not currently active.
   */
  <E extends Exception> void requireTransaction(ThrowingTransactionalWork<E> work)
      throws E, NoTransactionActiveException;
}
