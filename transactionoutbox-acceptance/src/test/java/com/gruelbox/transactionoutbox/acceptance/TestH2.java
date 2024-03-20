package com.gruelbox.transactionoutbox.acceptance;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gruelbox.transactionoutbox.*;
import com.gruelbox.transactionoutbox.testing.AbstractAcceptanceTest;
import com.gruelbox.transactionoutbox.testing.InterfaceProcessor;
import com.gruelbox.transactionoutbox.testing.LatchListener;
import com.gruelbox.transactionoutbox.testing.OrderedEntryListener;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

@SuppressWarnings("WeakerAccess")
class TestH2 extends AbstractAcceptanceTest {

  static final ThreadLocal<Boolean> inWrappedInvocation = ThreadLocal.withInitial(() -> false);

  @Test
  final void delayedExecutionImmediateSubmission() throws InterruptedException {

    CountDownLatch latch = new CountDownLatch(1);
    TransactionManager transactionManager = txManager();
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .instantiator(Instantiator.using(clazz -> (InterfaceProcessor) (foo, bar) -> {}))
            .listener(new OrderedEntryListener(latch, new CountDownLatch(1)))
            .persistor(Persistor.forDialect(connectionDetails().dialect()))
            .attemptFrequency(Duration.ofSeconds(60))
            .build();

    outbox.initialize();
    clearOutbox();

    var start = Instant.now();
    transactionManager.inTransaction(
        () ->
            outbox
                .with()
                .delayForAtLeast(Duration.ofSeconds(1))
                .schedule(InterfaceProcessor.class)
                .process(1, "bar"));
    assertTrue(latch.await(5, TimeUnit.SECONDS));
    assertTrue(start.plus(Duration.ofSeconds(1)).isBefore(Instant.now()));
  }

  @Test
  final void delayedExecutionFlushOnly() throws Exception {

    CountDownLatch latch = new CountDownLatch(1);
    TransactionManager transactionManager = txManager();
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .instantiator(Instantiator.using(clazz -> (InterfaceProcessor) (foo, bar) -> {}))
            .listener(new OrderedEntryListener(latch, new CountDownLatch(1)))
            .persistor(Persistor.forDialect(connectionDetails().dialect()))
            .attemptFrequency(Duration.ofSeconds(1))
            .build();

    outbox.initialize();
    clearOutbox();

    transactionManager.inTransaction(
        () ->
            outbox
                .with()
                .delayForAtLeast(Duration.ofSeconds(2))
                .schedule(InterfaceProcessor.class)
                .process(1, "bar"));
    assertFalse(latch.await(3, TimeUnit.SECONDS));

    withRunningFlusher(outbox, () -> assertTrue(latch.await(3, TimeUnit.SECONDS)));
  }

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
