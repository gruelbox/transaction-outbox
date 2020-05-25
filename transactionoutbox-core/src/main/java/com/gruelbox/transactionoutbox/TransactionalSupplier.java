package com.gruelbox.transactionoutbox;

@FunctionalInterface
public interface TransactionalSupplier<T, TX extends Transaction<?, ?>> {

  static <U, V extends Transaction<?, ?>> TransactionalSupplier<U, V> fromWork(
      TransactionalWork<V> work) {
    return transaction -> {
      work.doWork(transaction);
      return null;
    };
  }

  T doWork(TX transaction);
}
