package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.spi.BaseTransaction;

@Deprecated
@FunctionalInterface
public interface TransactionalSupplier<T>
    extends com.gruelbox.transactionoutbox.spi.TransactionalSupplier<T, Transaction> {

  @SuppressWarnings("unused")
  static <U, V extends BaseTransaction<?>> TransactionalSupplier<U> fromWork(
      TransactionalWork work) {
    return transaction -> {
      work.doWork(transaction);
      return null;
    };
  }
}
