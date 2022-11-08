package com.gruelbox.transactionoutbox;

@FunctionalInterface
public interface ThrowingTransactionalSupplier<T, E extends Exception> {

    static <F extends Exception> ThrowingTransactionalSupplier<Void, F> fromRunnable(
            Runnable runnable) {
        return transaction -> {
            runnable.run();
            return null;
        };
    }

    static <F extends Exception> ThrowingTransactionalSupplier<Void, F> fromWork(
            ThrowingTransactionalWork<F> work) {
        return transaction -> {
            work.doWork(transaction);
            return null;
        };
    }

    static ThrowingTransactionalSupplier<Void, RuntimeException> fromWork(TransactionalWork work) {
        return transaction -> {
            work.doWork(transaction);
            return null;
        };
    }

    static <T> ThrowingTransactionalSupplier<T, RuntimeException> fromSupplier(
            TransactionalSupplier<T> work) {
        return work::doWork;
    }

    T doWork(Transaction transaction) throws E;
}
