package com.gruelbox.transactionoutbox.jdbc;

import com.gruelbox.transactionoutbox.NoTransactionActiveException;
import com.gruelbox.transactionoutbox.spi.BaseTransactionManager;
import com.gruelbox.transactionoutbox.spi.ThrowingTransactionalSupplier;
import com.gruelbox.transactionoutbox.spi.ThrowingTransactionalWork;
import javax.sql.DataSource;

/**
 * A simple JDBC {@link BaseTransactionManager} implementation suitable for applications with no
 * existing transaction management. Uses thread-local transaction scopes and uses no transaction
 * context, leaving client code needing to interact directly with {@link
 * SimpleTransaction#connection()}.
 */
public interface SimpleTransactionManager extends JdbcTransactionManager<SimpleTransaction<Void>> {

  /**
   * Creates a simple transaction manager which uses the specified {@link DataSource} to source
   * connections. A new connection is requested for each transaction.
   *
   * <p>Transactions will be solely controlled through {@link JdbcTransactionManager}, so this may
   * be suitable for new applications with no other transaction management. Otherwise, a custom
   * {@link JdbcTransactionManager} implementation should be used.
   *
   * @param dataSource The data source.
   * @return The transaction manager.
   */
  static SimpleTransactionManager fromDataSource(DataSource dataSource) {
    return builder()
        .connectionProvider(
            DataSourceJdbcConnectionProvider.builder().dataSource(dataSource).build())
        .build();
  }

  /**
   * Creates a simple transaction manager which uses the specified connection details to request a
   * new connection from the {@link java.sql.DriverManager} every time a new transaction starts.
   *
   * <p>Transactions will be solely controlled through {@link JdbcTransactionManager}, and without
   * pooling, performance will be poor. Generally, {@link #fromDataSource(DataSource)} using a
   * pooling {@code DataSource} such as that provided by Hikari is preferred.
   *
   * @param driverClass The driver class name (e.g. {@code com.mysql.cj.jdbc.Driver}).
   * @param url The JDBC url.
   * @param username The username.
   * @param password The password.
   * @return The transaction manager.
   */
  static SimpleTransactionManager fromConnectionDetails(
      String driverClass, String url, String username, String password) {
    return builder()
        .connectionProvider(
            DriverJdbcConnectionProvider.builder()
                .driverClassName(driverClass)
                .url(url)
                .user(username)
                .password(password)
                .build())
        .build();
  }

  static SimpleTransactionManagerBuilder builder() {
    return new SimpleTransactionManagerImpl.Builder();
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
  <E extends Exception> void requireTransaction(
      ThrowingTransactionalWork<E, SimpleTransaction<Void>> work)
      throws E, NoTransactionActiveException;

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
      ThrowingTransactionalSupplier<T, E, SimpleTransaction<Void>> work)
      throws E, NoTransactionActiveException;

  interface SimpleTransactionManagerBuilder {
    SimpleTransactionManagerBuilder connectionProvider(JdbcConnectionProvider connectionProvider);

    SimpleTransactionManager build();
  }
}
