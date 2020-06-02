package com.gruelbox.transactionoutbox.jdbc;

import static com.gruelbox.transactionoutbox.Utils.uncheck;

import com.gruelbox.transactionoutbox.ThrowingTransactionalSupplier;
import com.gruelbox.transactionoutbox.spi.TransactionManager;
import java.sql.SQLException;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;

/**
 * A simple JDBC {@link TransactionManager} implementation suitable for applications with no
 * existing transaction management.
 */
@Slf4j
public final class SimpleTransactionManager
    extends AbstractThreadLocalJdbcTransactionManager<Void, SimpleTransaction<Void>> {

  private final JdbcConnectionProvider connectionProvider;

  private SimpleTransactionManager(JdbcConnectionProvider connectionProvider) {
    this.connectionProvider = connectionProvider;
  }

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
  public static SimpleTransactionManager fromDataSource(DataSource dataSource) {
    return new SimpleTransactionManager(
        DataSourceJdbcConnectionProvider.builder().dataSource(dataSource).build());
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
  public static SimpleTransactionManager fromConnectionDetails(
      String driverClass, String url, String username, String password) {
    return new SimpleTransactionManager(
        DriverJdbcConnectionProvider.builder()
            .driverClassName(driverClass)
            .url(url)
            .user(username)
            .password(password)
            .build());
  }

  @Override
  public <T, E extends Exception> T inTransactionReturnsThrows(
      ThrowingTransactionalSupplier<T, E, SimpleTransaction<Void>> work) throws E {
    return withTransaction(
        atx -> {
          T result = processAndCommitOrRollback(work, atx);
          atx.processHooks();
          return result;
        });
  }

  private <T, E extends Exception> T processAndCommitOrRollback(
      ThrowingTransactionalSupplier<T, E, SimpleTransaction<Void>> work,
      SimpleTransaction<Void> transaction)
      throws E {
    try {
      log.debug("Processing work");
      T result = work.doWork(transaction);
      transaction.flushBatches();
      log.debug("Committing transaction");
      transaction.commit();
      return result;
    } catch (Exception e) {
      try {
        log.warn(
            "Exception in transactional block ({}{}). Rolling back. See later messages for detail",
            e.getClass().getSimpleName(),
            e.getMessage() == null ? "" : (" - " + e.getMessage()));
        transaction.rollback();
      } catch (Exception ex) {
        log.warn("Failed to roll back", ex);
      }
      throw e;
    }
  }

  private <T, E extends Exception> T withTransaction(
      ThrowingTransactionalSupplier<T, E, SimpleTransaction<Void>> work) throws E {
    try (var connection = connectionProvider.obtainConnection();
        var transaction = pushTransaction(new SimpleTransaction<>(connection, null))) {
      log.debug("Got connection {}", connection);
      boolean autoCommit = transaction.connection().getAutoCommit();
      if (autoCommit) {
        log.debug("Setting auto-commit false");
        uncheck(() -> transaction.connection().setAutoCommit(false));
      }
      try {
        return work.doWork(transaction);
      } finally {
        connection.setAutoCommit(autoCommit);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    } finally {
      popTransaction();
    }
  }
}
