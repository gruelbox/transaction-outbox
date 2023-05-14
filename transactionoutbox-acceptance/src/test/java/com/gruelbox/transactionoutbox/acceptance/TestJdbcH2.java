package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.Instantiator;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import com.gruelbox.transactionoutbox.TransactionOutboxListener;
import com.gruelbox.transactionoutbox.Utils;
import com.gruelbox.transactionoutbox.sql.Dialects;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

class TestJdbcH2 extends AbstractSimpleTransactionManagerAcceptanceTest {

  static final ThreadLocal<Boolean> inWrappedInvocation = ThreadLocal.withInitial(() -> false);

  @Override
  protected JdbcConnectionDetails connectionDetails() {
    return JdbcConnectionDetails.builder()
        .dialect(Dialects.H2)
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
    TransactionOutbox outbox =
        builder()
            .instantiator(
                Instantiator.using(
                    clazz ->
                        (AsyncInterfaceProcessor)
                            (foo, bar, tx) -> {
                              if (!inWrappedInvocation.get()) {
                                return CompletableFuture.failedFuture(
                                    new IllegalStateException("Not in a wrapped invocation"));
                              }
                              return CompletableFuture.completedFuture(null);
                            }))
            .listener(
                new LatchListener(latch)
                    .andThen(
                        new TransactionOutboxListener() {
                          @Override
                          public Object wrapInvocation(Invocator invocator)
                              throws IllegalAccessException, IllegalArgumentException,
                                  InvocationTargetException {
                            inWrappedInvocation.set(true);
                            try {
                              return invocator.run();
                            } finally {
                              inWrappedInvocation.remove();
                            }
                          }
                        }))
            .build();

    cleanDataStore();

    Utils.join(
        txManager.transactionally(
            tx -> outbox.schedule(AsyncInterfaceProcessor.class).processAsync(1, "bar", tx)));
    Assertions.assertTrue(latch.await(5, TimeUnit.SECONDS));
  }

  @Test
  final void wrapInvocationsWithMdc() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    TransactionOutbox outbox =
        builder()
            .instantiator(
                Instantiator.using(
                    clazz ->
                        (AsyncInterfaceProcessor)
                            (foo, bar, tx) -> {
                              if (!Boolean.parseBoolean(MDC.get("BAR"))) {
                                return CompletableFuture.failedFuture(
                                    new IllegalStateException("Not in a wrapped invocation"));
                              }
                              return CompletableFuture.completedFuture(null);
                            }))
            .listener(
                new LatchListener(latch)
                    .andThen(
                        new TransactionOutboxListener() {
                          @Override
                          public Object wrapInvocation(Invocator invocator)
                              throws IllegalAccessException, IllegalArgumentException,
                                  InvocationTargetException {
                            MDC.put("BAR", "true");
                            try {
                              return invocator.run();
                            } finally {
                              MDC.remove("BAR");
                            }
                          }
                        }))
            .build();

    cleanDataStore();

    Utils.join(
        txManager.transactionally(
            tx -> outbox.schedule(AsyncInterfaceProcessor.class).processAsync(1, "bar", tx)));
    Assertions.assertTrue(latch.await(5, TimeUnit.SECONDS));
  }
}
