package com.gruelbox.transactionoutbox.jdbc;

import com.gruelbox.transactionoutbox.Beta;
import com.gruelbox.transactionoutbox.ThrowingTransactionalSupplier;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

/**
 * A stub JDBC transaction manager that assumes no underlying database and thread local transaction
 * management.
 */
@Slf4j
public class StubThreadLocalJdbcTransactionManager<CX, TX extends JdbcTransaction<CX>>
    extends AbstractThreadLocalJdbcTransactionManager<CX, TX> {

  private final Supplier<TX> transactionFactory;

  @Beta
  public StubThreadLocalJdbcTransactionManager(Supplier<TX> transactionFactory) {
    this.transactionFactory = transactionFactory;
  }

  @Override
  public <T, E extends Exception> T inTransactionReturnsThrows(
      ThrowingTransactionalSupplier<T, E, TX> work) throws E {
    return withTransaction(
        atx -> {
          T result = work.doWork(atx);
          if (atx instanceof SimpleTransaction) {
            ((SimpleTransaction) atx).processHooks();
          }
          return result;
        });
  }

  private <T, E extends Exception> T withTransaction(ThrowingTransactionalSupplier<T, E, TX> work)
      throws E {
    var transaction = pushTransaction(transactionFactory.get());
    try {
      return work.doWork(transaction);
    } finally {
      popTransaction();
    }
  }
}
