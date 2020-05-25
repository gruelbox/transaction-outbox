package com.gruelbox.transactionoutbox;

@FunctionalInterface
public interface ThrowingTransactionalWork<E extends Exception, TX extends Transaction<?, ?>> {

  void doWork(TX transaction) throws E;
}
