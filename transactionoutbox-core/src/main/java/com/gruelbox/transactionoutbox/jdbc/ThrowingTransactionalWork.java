package com.gruelbox.transactionoutbox.jdbc;

import com.gruelbox.transactionoutbox.Transaction;

@FunctionalInterface
public interface ThrowingTransactionalWork<E extends Exception, TX extends Transaction<?, ?>> {

  void doWork(TX transaction) throws E;
}
