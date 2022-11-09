package com.synaos.transactionoutbox.acceptance;

import com.synaos.transactionoutbox.*;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

@SuppressWarnings("WeakerAccess")
class TestH2 extends AbstractAcceptanceTest {

    static final ThreadLocal<Boolean> inWrappedInvocation = ThreadLocal.withInitial(() -> false);

    @Override
    protected ConnectionDetails connectionDetails() {
        return ConnectionDetails.builder()
                .dialect(Dialect.H2)
                .driverClassName("org.h2.Driver")
                .url(
                        "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;DEFAULT_LOCK_TIMEOUT=60000;LOB_TIMEOUT=2000;MV_STORE=TRUE")
                .user("test")
                .password("test")
                .build();
    }

    @Test
    final void wrapInvocations() throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(1);
        TransactionManager transactionManager = simpleTxnManager();
        TransactionOutbox outbox =
                TransactionOutbox.builder()
                        .transactionManager(transactionManager)
                        .instantiator(
                                Instantiator.using(
                                        clazz ->
                                                (InterfaceProcessor)
                                                        (foo, bar) -> {
                                                            if (!inWrappedInvocation.get()) {
                                                                throw new IllegalStateException("Not in a wrapped invocation");
                                                            }
                                                        }))
                        .listener(
                                new LatchListener(latch)
                                        .andThen(
                                                new TransactionOutboxListener() {
                                                    @Override
                                                    public void wrapInvocation(Invocator invocator)
                                                            throws IllegalAccessException, IllegalArgumentException,
                                                            InvocationTargetException {
                                                        inWrappedInvocation.set(true);
                                                        try {
                                                            invocator.run();
                                                        } finally {
                                                            inWrappedInvocation.remove();
                                                        }
                                                    }
                                                }))
                        .persistor(Persistor.forDialect(connectionDetails().dialect()))
                        .build();

        outbox.initialize();
        clearOutbox();

        transactionManager.inTransaction(
                () -> outbox.schedule(InterfaceProcessor.class).process(1, "bar"));
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    @Test
    final void wrapInvocationsWithMDC() throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(1);
        TransactionManager transactionManager = simpleTxnManager();
        TransactionOutbox outbox =
                TransactionOutbox.builder()
                        .transactionManager(transactionManager)
                        .instantiator(
                                Instantiator.using(
                                        clazz ->
                                                (InterfaceProcessor)
                                                        (foo, bar) -> {
                                                            if (!Boolean.parseBoolean(MDC.get("BAR"))) {
                                                                throw new IllegalStateException("Not in a wrapped invocation");
                                                            }
                                                        }))
                        .listener(
                                new LatchListener(latch)
                                        .andThen(
                                                new TransactionOutboxListener() {
                                                    @Override
                                                    public void wrapInvocation(Invocator invocator)
                                                            throws IllegalAccessException, IllegalArgumentException,
                                                            InvocationTargetException {
                                                        MDC.put("BAR", "true");
                                                        try {
                                                            invocator.run();
                                                        } finally {
                                                            MDC.remove("BAR");
                                                        }
                                                    }
                                                }))
                        .persistor(Persistor.forDialect(connectionDetails().dialect()))
                        .build();

        outbox.initialize();
        clearOutbox();

        transactionManager.inTransaction(
                () -> outbox.schedule(InterfaceProcessor.class).process(1, "bar"));
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }
}
