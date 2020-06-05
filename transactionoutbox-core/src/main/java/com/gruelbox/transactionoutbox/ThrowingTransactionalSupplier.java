package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.spi.BaseTransaction;

@Deprecated
@FunctionalInterface
public interface ThrowingTransactionalSupplier<T, E extends Exception>
    extends com.gruelbox.transactionoutbox.spi.ThrowingTransactionalSupplier<T, E, Transaction> {

  static <F extends Exception> ThrowingTransactionalSupplier<Void, F> fromRunnable(
      Runnable runnable) {
    return transaction -> {
      runnable.run();
      return null;
    };
  }

  static <F extends Exception> ThrowingTransactionalSupplier<Void, F> fromWork(
      ThrowingTransactionalWork<F> work) {
    return transaction -> {
      work.doWork(transaction);
      return null;
    };
  }

  static <G extends BaseTransaction<?>>
      ThrowingTransactionalSupplier<Void, RuntimeException> fromWork(TransactionalWork work) {
    return transaction -> {
      work.doWork(transaction);
      return null;
    };
  }

  static <T, G extends BaseTransaction<?>>
      ThrowingTransactionalSupplier<T, RuntimeException> fromSupplier(
          TransactionalSupplier<T> work) {
    return work::doWork;
  }
}
