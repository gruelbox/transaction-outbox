package com.gruelbox.transactionoutbox;

@Deprecated
@FunctionalInterface
public interface TransactionalWork
    extends com.gruelbox.transactionoutbox.spi.TransactionalWork<Transaction> {}
