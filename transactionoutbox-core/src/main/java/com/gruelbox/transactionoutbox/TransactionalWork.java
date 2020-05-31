package com.gruelbox.transactionoutbox;


@FunctionalInterface
public interface TransactionalWork<TX extends Transaction<?, ?>> {

  void doWork(TX transaction);
}
