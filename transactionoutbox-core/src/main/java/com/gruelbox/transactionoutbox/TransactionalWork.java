package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.Transaction;

@FunctionalInterface
public interface TransactionalWork<TX extends Transaction<?, ?>> {

  void doWork(TX transaction);
}
