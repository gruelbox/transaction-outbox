package com.gruelbox.transactionoutbox.jdbc;

import com.gruelbox.transactionoutbox.Transaction;

@FunctionalInterface
public interface TransactionalWork<TX extends Transaction<?, ?>> {

  void doWork(TX transaction);
}
