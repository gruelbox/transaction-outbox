package com.gruelbox.transactionoutbox;

import java.lang.reflect.Method;

/**
 * A transaction manager which assumes there is a single "current" {@link Transaction} on a thread
 * (presumably saved in a {@link ThreadLocal}) which can be both used by {@link
 * TransactionOutbox#schedule(Class)} as the current context to write records using {@link
 * Persistor} <em>and</em> used by scheduled methods themselves to write changes within the
 * transaction started as a result of reading and locking the request.
 *
 * <p>Call pattern permitted:
 *
 * <pre>transactionManager.inTransaction(() -&gt; outbox.schedule(MyClass.ckass).myMethod("foo");
 * </pre>
 *
 * <p>Adds the {@link #requireTransactionReturns(ThrowingTransactionalSupplier)} and {@link
 * #requireTransaction(ThrowingTransactionalWork)} methods, which extract the current transaction
 * from the thread context and pass it on, throwing {@link NoTransactionActiveException} if there is
 * no current transaction.
 */
public interface ThreadLocalContextTransactionManager extends TransactionManager {

    /**
     * Runs the specified work in the context of the "current" transaction (the definition of which is
     * up to the implementation).
     *
     * @param work Code which must be called while the transaction is active.
     * @param <E>  The exception type.
     * @throws E                            If any exception is thrown by {@link Runnable}.
     * @throws NoTransactionActiveException If a transaction is not currently active.
     */
    default <E extends Exception> void requireTransaction(ThrowingTransactionalWork<E> work)
            throws E, NoTransactionActiveException {
        requireTransactionReturns(ThrowingTransactionalSupplier.fromWork(work));
    }

    /**
     * Runs the specified work in the context of the "current" transaction (the definition of which is
     * up to the implementation).
     *
     * @param work Code which must be called while the transaction is active.
     * @param <T>  The type returned.
     * @param <E>  The exception type.
     * @return The value returned by {@code work}.
     * @throws E                             If any exception is thrown by {@link Runnable}.
     * @throws NoTransactionActiveException  If a transaction is not currently active.
     * @throws UnsupportedOperationException If the transaction manager does not support thread-local
     *                                       context.
     */
    <T, E extends Exception> T requireTransactionReturns(ThrowingTransactionalSupplier<T, E> work)
            throws E, NoTransactionActiveException;

    /**
     * Obtains the active transaction by using {@link
     * #requireTransactionReturns(ThrowingTransactionalSupplier)}, thus requiring nothing to be passed
     * in the method invocation. No changes are made to the invocation.
     *
     * @param method The method called.
     * @param args   The method arguments.
     * @return The transactional invocation.
     */
    @Override
    default TransactionalInvocation extractTransaction(Method method, Object[] args) {
        return requireTransactionReturns(
                transaction ->
                        new TransactionalInvocation(
                                method.getDeclaringClass(),
                                method.getName(),
                                method.getParameterTypes(),
                                args,
                                transaction));
    }

    /**
     * The transaction is not needed as part of an invocation, so the invocation is left unmodified.
     *
     * @param invocation The invocation.
     * @return The unmodified invocation.
     */
    @Override
    default Invocation injectTransaction(Invocation invocation, Transaction transaction) {
        return invocation;
    }
}
