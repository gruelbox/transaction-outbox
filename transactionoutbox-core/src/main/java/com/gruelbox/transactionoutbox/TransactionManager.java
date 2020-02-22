package com.gruelbox.transactionoutbox;

import static com.gruelbox.transactionoutbox.Utils.uncheck;
import static com.gruelbox.transactionoutbox.Utils.uncheckedly;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
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
   * @return The connection to use on the current thread. This will be called back during a call to
   *     {@link TransactionOutbox#schedule(Class)} and is expected to be set to {@code autoCommit =
   *     false}. The caller is expected to commit the active transaction.
   * @throws NoTransactionActiveException If there is no transaction active.
   */
  Connection getActiveConnection(); /* TODO replace with withConnection? */

  /**
   * Creates a prepared statement which will be cached and re-used within a transaction. Any batch
   * on these statements is executed before the transaction is committed, and automatically closed.
   *
   * @param sql The SQL statement
   * @return The statement.
   */
  PreparedStatement prepareBatchStatement(String sql);

  /**
   * Will be called back by {@link TransactionOutbox#flush()}. Should do any work necessary to
   * obtain a connection (which must be then made available to {@link #getActiveConnection()}),
   * start a (new) transaction, call {@code runnable} and then either commit on success or rollback
   * on failure.
   *
   * @param runnable Code which must be called while the transaction is active.
   * @throws TransactionAlreadyActiveException If the transaction manager can't handle stacked,
   *     suspended transactions.
   */
  default void inTransaction(Runnable runnable) {
    uncheck(() -> inTransactionReturnsThrows(Executors.callable(runnable)));
  }

  /**
   * Will be called back by {@link TransactionOutbox#flush()}. Should do any work necessary to
   * obtain a connection (which must be then made available to {@link #getActiveConnection()}),
   * start a (new) transaction, call {@code runnable} and then either commit on success or rollback
   * on failure.
   *
   * @param <T> The type returned.
   * @param supplier Code which must be called while the transaction is active.
   * @return The result of {@code supplier}.
   * @throws TransactionAlreadyActiveException If the transaction manager can't handle stacked,
   *     suspended transactions.
   */
  default <T> T inTransactionReturns(Supplier<T> supplier) {
    return uncheckedly(() -> inTransactionReturnsThrows(supplier::get));
  }

  /**
   * Will be called back by {@link TransactionOutbox#flush()}. Should do any work necessary to
   * obtain a connection (which must be then made available to {@link #getActiveConnection()}),
   * start a (new) transaction, call {@code runnable} and then either commit on success or rollback
   * on failure.
   *
   * @param runnable Code which must be called while the transaction is active.
   * @throws TransactionAlreadyActiveException If the transaction manager can't handle stacked,
   *     suspended transactions.
   * @throws Exception If any exception is thrown by {@link Runnable}.
   */
  @SuppressWarnings("SameReturnValue")
  default void inTransactionThrows(ThrowingRunnable runnable) throws Exception {
    inTransactionReturnsThrows(
        () -> {
          runnable.run();
          return null;
        });
  }

  /**
   * Will be called back by {@link TransactionOutbox#flush()}. Should do any work necessary to
   * obtain a connection (which must be then made available to {@link #getActiveConnection()}),
   * start a (new) transaction, call {@code runnable} and then either commit on success or rollback
   * on failure.
   *
   * @param <T> The type returned.
   * @param callable Code which must be called while the transaction is active.
   * @return The result of {@code supplier}.
   * @throws TransactionAlreadyActiveException If the transaction manager can't handle stacked,
   *     suspended transactions.
   * @throws Exception If any exception is thrown by {@link Runnable}.
   */
  <T> T inTransactionReturnsThrows(Callable<T> callable) throws Exception;

  /**
   * Will be called to perform work immediately after the current transaction is committed. This
   * should occur in the same thread and will generally not be long-lasting.
   *
   * @param runnable The code to run post-commit.
   */
  void addPostCommitHook(Runnable runnable);
}
