package com.gruelbox.transactionoutbox;

@FunctionalInterface
public interface TransactionalSupplier<T> {

    static <U> TransactionalSupplier<U> fromWork(TransactionalWork work) {
        return transaction -> {
            work.doWork(transaction);
            return null;
        };
    }

    T doWork(Transaction transaction);
}
