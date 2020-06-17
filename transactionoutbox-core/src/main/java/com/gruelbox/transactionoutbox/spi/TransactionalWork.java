package com.gruelbox.transactionoutbox.spi;

@FunctionalInterface
public interface TransactionalWork<TX extends BaseTransaction<?>> {

  void doWork(TX transaction);
}
