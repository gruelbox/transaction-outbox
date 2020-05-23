package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.spi.BaseTransaction;

@FunctionalInterface
public interface TransactionalSupplier<T, TX extends BaseTransaction<?>> {

  static <U, V extends BaseTransaction<?>> TransactionalSupplier<U, V> fromWork(
      TransactionalWork<V> work) {
    return transaction -> {
      work.doWork(transaction);
      return null;
    };
  }

  T doWork(TX transaction);
}
