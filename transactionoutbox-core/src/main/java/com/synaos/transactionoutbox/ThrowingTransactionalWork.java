package com.synaos.transactionoutbox;

@FunctionalInterface
public interface ThrowingTransactionalWork<E extends Exception> {

    void doWork(Transaction transaction) throws E;
}
