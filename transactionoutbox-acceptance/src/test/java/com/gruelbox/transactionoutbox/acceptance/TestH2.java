package com.gruelbox.transactionoutbox.acceptance;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gruelbox.transactionoutbox.*;
import com.gruelbox.transactionoutbox.testing.AbstractAcceptanceTest;
import com.gruelbox.transactionoutbox.testing.InterfaceProcessor;
import com.gruelbox.transactionoutbox.testing.LatchListener;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

@SuppressWarnings("WeakerAccess")
class TestH2 extends AbstractAcceptanceTest {

  static final ThreadLocal<Boolean> inWrappedInvocation = ThreadLocal.withInitial(() -> false);

  @Test
  final void wrapInvocations() throws InterruptedException {

    CountDownLatch latch = new CountDownLatch(1);
    TransactionManager transactionManager = txManager();
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
                              throws IllegalAccessException,
                                  IllegalArgumentException,
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
    TransactionManager transactionManager = txManager();
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
                              throws IllegalAccessException,
                                  IllegalArgumentException,
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
