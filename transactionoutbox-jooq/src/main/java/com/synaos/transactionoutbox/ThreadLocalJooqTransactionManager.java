package com.synaos.transactionoutbox;

import lombok.extern.slf4j.Slf4j;
import org.jooq.Configuration;
import org.jooq.DSLContext;

/**
 * jOOQ transaction manager which uses thread-local context. Best used with {@link
 * org.jooq.impl.ThreadLocalTransactionProvider}. Relies on a {@link JooqTransactionListener} being
 * attached to the {@link DSLContext}.
 */
@Slf4j
final class ThreadLocalJooqTransactionManager
        extends AbstractThreadLocalTransactionManager<SimpleTransaction>
        implements JooqTransactionManager {

    private final DSLContext parentDsl;

    ThreadLocalJooqTransactionManager(DSLContext parentDsl) {
        this.parentDsl = parentDsl;
    }

    @Override
    public <T, E extends Exception> T inTransactionReturnsThrows(
            ThrowingTransactionalSupplier<T, E> work) {
        DSLContext dsl =
                peekTransaction()
                        .map(SimpleTransaction::context)
                        .map(Configuration.class::cast)
                        .map(Configuration::dsl)
                        .orElse(parentDsl);
        return dsl.transactionResult(
                config ->
                        config
                                .dsl()
                                .connectionResult(connection -> work.doWork(peekTransaction().orElseThrow())));
    }
}
