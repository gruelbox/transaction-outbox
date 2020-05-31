package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.jdbc.TransactionalSupplier;

@FunctionalInterface
public interface ThrowingTransactionalSupplier<
    T, E extends Exception, TX extends Transaction<?, ?>> {

  static <F extends Exception, G extends Transaction<?, ?>>
      ThrowingTransactionalSupplier<Void, F, G> fromRunnable(Runnable runnable) {
    return transaction -> {
      runnable.run();
      return null;
    };
  }

  static <F extends Exception, G extends Transaction<?, ?>>
      ThrowingTransactionalSupplier<Void, F, G> fromWork(ThrowingTransactionalWork<F, G> work) {
    return transaction -> {
      work.doWork(transaction);
      return null;
    };
  }

  static <G extends Transaction<?, ?>>
      ThrowingTransactionalSupplier<Void, RuntimeException, G> fromWork(TransactionalWork<G> work) {
    return transaction -> {
      work.doWork(transaction);
      return null;
    };
  }

  static <T, G extends Transaction<?, ?>>
      ThrowingTransactionalSupplier<T, RuntimeException, G> fromSupplier(
          TransactionalSupplier<T, G> work) {
    return work::doWork;
  }

  T doWork(TX transaction) throws E;
}
