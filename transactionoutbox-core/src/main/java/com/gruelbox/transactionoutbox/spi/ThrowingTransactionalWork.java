package com.gruelbox.transactionoutbox.spi;

@FunctionalInterface
public interface ThrowingTransactionalWork<E extends Exception, TX extends BaseTransaction<?>> {

  void doWork(TX transaction) throws E;
}
