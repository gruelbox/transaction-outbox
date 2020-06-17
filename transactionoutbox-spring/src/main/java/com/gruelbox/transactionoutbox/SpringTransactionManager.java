package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.jdbc.JdbcTransactionManager;
import com.gruelbox.transactionoutbox.spi.ThrowingTransactionalSupplier;
import com.gruelbox.transactionoutbox.spi.ThrowingTransactionalWork;

/** Transaction manager which uses spring-tx and Hibernate. */
public interface SpringTransactionManager extends JdbcTransactionManager<SpringTransaction> {

  /** @deprecated Unnecessary, provided for backwards compatibility. */
  @Deprecated
  default <E extends Exception> void requireTransaction(
      ThrowingTransactionalWork<E, SpringTransaction> work) throws E, NoTransactionActiveException {
    requireTransactionReturns(ThrowingTransactionalSupplier.fromWork(work));
  }

  /** @deprecated Unnecessary, provided for backwards compatibility. */
  @Deprecated
  <T, E extends Exception> T requireTransactionReturns(
      ThrowingTransactionalSupplier<T, E, SpringTransaction> work)
      throws E, NoTransactionActiveException;
}
