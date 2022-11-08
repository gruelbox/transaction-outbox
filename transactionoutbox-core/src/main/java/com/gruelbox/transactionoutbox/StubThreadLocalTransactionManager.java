package com.gruelbox.transactionoutbox;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;

/**
 * A stub transaction manager that assumes no underlying database and thread local transaction
 * management.
 */
@Slf4j
public class StubThreadLocalTransactionManager
        extends AbstractThreadLocalTransactionManager<SimpleTransaction> {

    @Beta
    public StubThreadLocalTransactionManager() {
        // Nothing to do
    }

    @Override
    public <T, E extends Exception> T inTransactionReturnsThrows(
            ThrowingTransactionalSupplier<T, E> work) throws E {
        return withTransaction(
                atx -> {
                    T result = work.doWork(atx);
                    ((SimpleTransaction) atx).processHooks();
                    return result;
                });
    }

    private <T, E extends Exception> T withTransaction(ThrowingTransactionalSupplier<T, E> work)
            throws E {
        Connection mockConnection = Utils.createLoggingProxy(new ProxyFactory(), Connection.class);
        try (SimpleTransaction transaction =
                     pushTransaction(new SimpleTransaction(mockConnection, null))) {
            return work.doWork(transaction);
        } finally {
            popTransaction();
        }
    }
}
