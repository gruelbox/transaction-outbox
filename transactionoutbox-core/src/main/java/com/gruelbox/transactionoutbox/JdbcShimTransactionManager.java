package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.jdbc.SimpleTransactionManager;

class JdbcShimTransactionManager implements ThreadLocalContextTransactionManager {

  private final SimpleTransactionManager delegate;

  JdbcShimTransactionManager(SimpleTransactionManager transactionManager) {
    delegate = transactionManager;
  }

  @Override
  public <T, E extends Exception> T requireTransactionReturns(
      ThrowingTransactionalSupplier<T, E, Transaction> work)
      throws E, NoTransactionActiveException {
    return delegate.requireTransactionReturns(tx -> work.doWork(new JdbcShimTransaction(tx)));
  }

  @Override
  public <T, E extends Exception> T inTransactionReturnsThrows(
      ThrowingTransactionalSupplier<T, E, Transaction> work) throws E {
    return delegate.inTransactionReturnsThrows(tx -> work.doWork(new JdbcShimTransaction(tx)));
  }
}
