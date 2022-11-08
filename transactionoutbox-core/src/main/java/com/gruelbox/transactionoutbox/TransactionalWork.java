package com.gruelbox.transactionoutbox;

@FunctionalInterface
public interface TransactionalWork {

    void doWork(Transaction transaction);
}
