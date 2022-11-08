package com.synaos.transactionoutbox;

@FunctionalInterface
public interface TransactionalWork {

    void doWork(Transaction transaction);
}
