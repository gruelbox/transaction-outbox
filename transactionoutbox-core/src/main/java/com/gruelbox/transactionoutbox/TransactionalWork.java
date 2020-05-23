package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.spi.BaseTransaction;

@FunctionalInterface
public interface TransactionalWork<TX extends BaseTransaction<?>> {

  void doWork(TX transaction);
}
