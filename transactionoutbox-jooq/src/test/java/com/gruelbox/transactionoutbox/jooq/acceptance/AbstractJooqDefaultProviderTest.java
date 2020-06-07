package com.gruelbox.transactionoutbox.jooq.acceptance;

import static com.gruelbox.transactionoutbox.jooq.acceptance.TestUtils.uncheck;
import static java.util.concurrent.CompletableFuture.runAsync;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gruelbox.transactionoutbox.DefaultJooqTransactionManager;
import com.gruelbox.transactionoutbox.JooqTransaction;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.TransactionOutboxListener;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;

abstract class AbstractJooqDefaultProviderTest
    extends AbstractJooqAcceptanceTest<DefaultJooqTransactionManager> {

  protected static DSLContext dsl;

  @Override
  protected boolean supportsThreadLocalContext() {
    return false;
  }

  @Test
  void testNestedPassingContext() throws Exception {
    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(1);
    TransactionOutbox outbox =
        builder()
            .attemptFrequency(Duration.of(1, ChronoUnit.SECONDS))
            .listener(
                new TransactionOutboxListener() {
                  @Override
                  public void success(TransactionOutboxEntry entry) {
                    if (entry.getInvocation().getArgs()[0].equals(1)) {
                      latch1.countDown();
                    } else {
                      latch2.countDown();
                    }
                  }
                })
            .build();

    cleanDataStore();

    withRunningFlusher(
        outbox,
        () -> {
          dsl.transaction(
              ctx -> {
                outbox.schedule(Worker.class).process(1, ctx);
                ctx.dsl().transaction(cx1 -> outbox.schedule(Worker.class).process(2, ctx));

                // Neither should be fired - the second job is in a nested transaction
                CompletableFuture.allOf(
                        runAsync(
                            () -> uncheck(() -> assertFalse(latch1.await(2, TimeUnit.SECONDS)))),
                        runAsync(
                            () -> uncheck(() -> assertFalse(latch2.await(2, TimeUnit.SECONDS)))))
                    .get();
              });

          // Both should be fired after commit
          CompletableFuture.allOf(
                  runAsync(() -> uncheck(() -> assertTrue(latch1.await(2, TimeUnit.SECONDS)))),
                  runAsync(() -> uncheck(() -> assertTrue(latch2.await(2, TimeUnit.SECONDS)))))
              .get();
        });
    TestUtils.assertRecordExists(dsl, 1);
    TestUtils.assertRecordExists(dsl, 2);
  }

  @Test
  void testNestedPassingTransaction() throws Exception {
    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(1);
    TransactionOutbox outbox =
        builder()
            .attemptFrequency(Duration.of(1, ChronoUnit.SECONDS))
            .listener(
                new TransactionOutboxListener() {
                  @Override
                  public void success(TransactionOutboxEntry entry) {
                    if (entry.getInvocation().getArgs()[0].equals(1)) {
                      latch1.countDown();
                    } else {
                      latch2.countDown();
                    }
                  }
                })
            .build();

    cleanDataStore();

    withRunningFlusher(
        outbox,
        () -> {
          txManager.inTransactionThrows(
              tx1 -> {
                outbox.schedule(Worker.class).process(1, tx1);

                txManager.inTransactionThrows(tx2 -> outbox.schedule(Worker.class).process(2, tx2));

                // The inner transaction should be committed - these are different semantics
                CompletableFuture.allOf(
                        runAsync(
                            () -> uncheck(() -> assertFalse(latch1.await(2, TimeUnit.SECONDS)))),
                        runAsync(
                            () -> uncheck(() -> assertTrue(latch2.await(2, TimeUnit.SECONDS)))))
                    .get();

                TestUtils.assertRecordExists(dsl, 2);
              });

          // Should be fired after commit
          assertTrue(latch1.await(2, TimeUnit.SECONDS));
        });

    TestUtils.assertRecordExists(dsl, 1);
  }

  /**
   * Ensures that given the rollback of an inner transaction, any outbox work scheduled in the inner
   * transaction is rolled back while the outer transaction's works.
   */
  @Test
  void testNestedWithInnerFailure() throws Exception {
    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(1);
    TransactionOutbox outbox =
        builder()
            .attemptFrequency(Duration.of(1, ChronoUnit.SECONDS))
            .listener(
                new TransactionOutboxListener() {
                  @Override
                  public void success(TransactionOutboxEntry entry) {
                    if (entry.getInvocation().getArgs()[0].equals(1)) {
                      latch1.countDown();
                    } else {
                      latch2.countDown();
                    }
                  }
                })
            .build();

    cleanDataStore();

    withRunningFlusher(
        outbox,
        () -> {
          dsl.transaction(
              ctx -> {
                outbox.schedule(Worker.class).process(1, ctx);

                assertThrows(
                    UnsupportedOperationException.class,
                    () ->
                        ctx.dsl()
                            .transaction(
                                cx2 -> {
                                  outbox.schedule(Worker.class).process(2, cx2);
                                  throw new UnsupportedOperationException();
                                }));

                CompletableFuture.allOf(
                        runAsync(
                            () -> uncheck(() -> assertFalse(latch1.await(2, TimeUnit.SECONDS)))),
                        runAsync(
                            () -> uncheck(() -> assertFalse(latch2.await(2, TimeUnit.SECONDS)))))
                    .get();
              });

          CompletableFuture.allOf(
                  runAsync(() -> uncheck(() -> assertTrue(latch1.await(2, TimeUnit.SECONDS)))),
                  runAsync(() -> uncheck(() -> assertFalse(latch2.await(2, TimeUnit.SECONDS)))))
              .get();
        });
    TestUtils.assertRecordExists(dsl, 1);
    TestUtils.assertRecordNotExists(dsl, 2);
  }

  @SuppressWarnings("EmptyMethod")
  static class Worker {

    @SuppressWarnings("SameParameterValue")
    void process(int i, JooqTransaction transaction) {
      TestUtils.writeRecord(transaction, i);
    }

    void process(int i, Configuration configuration) {
      TestUtils.writeRecord(configuration, i);
    }
  }
}
