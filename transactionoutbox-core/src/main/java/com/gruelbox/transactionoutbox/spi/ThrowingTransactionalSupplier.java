package com.gruelbox.transactionoutbox.spi;

@FunctionalInterface
public interface ThrowingTransactionalSupplier<
    T, E extends Exception, TX extends BaseTransaction<?>> {

  static <F extends Exception, G extends BaseTransaction<?>>
      ThrowingTransactionalSupplier<Void, F, G> fromRunnable(Runnable runnable) {
    return transaction -> {
      runnable.run();
      return null;
    };
  }

  static <F extends Exception, G extends BaseTransaction<?>>
      ThrowingTransactionalSupplier<Void, F, G> fromWork(ThrowingTransactionalWork<F, G> work) {
    return transaction -> {
      work.doWork(transaction);
      return null;
    };
  }

  static <G extends BaseTransaction<?>>
      ThrowingTransactionalSupplier<Void, RuntimeException, G> fromWork(TransactionalWork<G> work) {
    return transaction -> {
      work.doWork(transaction);
      return null;
    };
  }

  static <T, G extends BaseTransaction<?>>
      ThrowingTransactionalSupplier<T, RuntimeException, G> fromSupplier(
          TransactionalSupplier<T, G> work) {
    return work::doWork;
  }

  T doWork(TX transaction) throws E;
}
