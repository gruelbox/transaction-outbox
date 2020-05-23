package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.spi.BaseTransaction;

@FunctionalInterface
public interface ThrowingTransactionalWork<E extends Exception, TX extends BaseTransaction<?>> {

  void doWork(TX transaction) throws E;
}
