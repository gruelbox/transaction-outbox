package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.jdbc.AbstractThreadLocalJdbcTransactionManager;
import com.gruelbox.transactionoutbox.jdbc.SimpleTransaction;
import java.sql.Connection;
import lombok.extern.slf4j.Slf4j;

/**
 * A stub transaction manager that assumes no underlying database and thread local transaction
 * management.
 */
@Slf4j
@Deprecated
public class StubThreadLocalTransactionManager
    extends AbstractThreadLocalJdbcTransactionManager<Transaction>
    implements ThreadLocalContextTransactionManager {

  public StubThreadLocalTransactionManager() {
    // Nothing to do
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T, E extends Exception> T inTransactionReturnsThrows(
      ThrowingTransactionalSupplier<T, E> work) throws E {
    return withTransaction(
        atx -> {
          T result = work.doWork(atx);
          ((SimpleTransaction<Void>) ((JdbcShimTransaction) atx).getDelegate()).processHooks();
          return result;
        });
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T, E extends Exception> T inTransactionReturnsThrows(
      com.gruelbox.transactionoutbox.spi.ThrowingTransactionalSupplier<T, E, Transaction> work)
      throws E {
    return withTransaction(
        atx -> {
          T result = work.doWork(atx);
          ((SimpleTransaction<Void>) ((JdbcShimTransaction) atx).getDelegate()).processHooks();
          return result;
        });
  }

  @Override
  public <T, E extends Exception> T requireTransactionReturns(
      ThrowingTransactionalSupplier<T, E> work) throws E, NoTransactionActiveException {
    return super.requireTransactionReturns(work);
  }

  private <T, E extends Exception> T withTransaction(ThrowingTransactionalSupplier<T, E> work)
      throws E {
    Connection mockConnection = Utils.createLoggingProxy(Connection.class);
    try (SimpleTransaction<Void> tx = new SimpleTransaction<>(mockConnection, null)) {
      JdbcShimTransaction shim = new JdbcShimTransaction(tx);
      pushTransaction(shim);
      try {
        return work.doWork(shim);
      } finally {
        popTransaction();
      }
    }
  }
}
