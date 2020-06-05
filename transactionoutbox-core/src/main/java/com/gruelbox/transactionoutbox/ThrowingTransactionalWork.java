package com.gruelbox.transactionoutbox;

@Deprecated
@FunctionalInterface
public interface ThrowingTransactionalWork<E extends Exception>
    extends com.gruelbox.transactionoutbox.spi.ThrowingTransactionalWork<E, Transaction> {}
