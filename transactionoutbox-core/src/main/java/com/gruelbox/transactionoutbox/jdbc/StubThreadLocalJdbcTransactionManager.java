package com.gruelbox.transactionoutbox.jdbc;

import com.gruelbox.transactionoutbox.Beta;
import com.gruelbox.transactionoutbox.ThrowingTransactionalSupplier;
import com.gruelbox.transactionoutbox.Utils;
import java.sql.Connection;
import lombok.extern.slf4j.Slf4j;

/**
 * A stub JDBC transaction manager that assumes no underlying database and thread local transaction
 * management.
 */
@Slf4j
public class StubThreadLocalJdbcTransactionManager
    extends AbstractThreadLocalJdbcTransactionManager<Void, SimpleTransaction<Void>> {

  @Beta
  public StubThreadLocalJdbcTransactionManager() {
    // Nothing to do
  }

  @Override
  public <T, E extends Exception> T inTransactionReturnsThrows(
      ThrowingTransactionalSupplier<T, E, SimpleTransaction<Void>> work) throws E {
    return withTransaction(
        atx -> {
          T result = work.doWork(atx);
          atx.processHooks();
          return result;
        });
  }

  private <T, E extends Exception> T withTransaction(
      ThrowingTransactionalSupplier<T, E, SimpleTransaction<Void>> work) throws E {
    Connection mockConnection = Utils.createLoggingProxy(Connection.class);
    try (var transaction = pushTransaction(new SimpleTransaction<>(mockConnection, null))) {
      return work.doWork(transaction);
    } finally {
      popTransaction();
    }
  }
}
