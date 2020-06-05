package com.gruelbox.transactionoutbox.spi;

@FunctionalInterface
public interface TransactionalSupplier<T, TX extends BaseTransaction<?>> {

  public static <U, V extends BaseTransaction<?>> TransactionalSupplier<U, V> fromWork(
      TransactionalWork<V> work) {
    return transaction -> {
      work.doWork(transaction);
      return null;
    };
  }

  T doWork(TX transaction);
}
