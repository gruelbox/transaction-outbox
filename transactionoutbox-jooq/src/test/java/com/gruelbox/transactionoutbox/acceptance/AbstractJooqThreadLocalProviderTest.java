package com.gruelbox.transactionoutbox.acceptance;

import static com.gruelbox.transactionoutbox.acceptance.TestUtils.uncheck;
import static java.util.concurrent.CompletableFuture.runAsync;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.gruelbox.transactionoutbox.Instantiator;
import com.gruelbox.transactionoutbox.ThreadLocalJooqTransactionManager;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.TransactionOutboxListener;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;

abstract class AbstractJooqThreadLocalProviderTest
    extends AbstractJooqAcceptanceTest<ThreadLocalJooqTransactionManager> {

  protected static DSLContext dsl;

  @Test
  void testJooqNestedDirectInvocation() throws Exception {
    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(1);
    TransactionOutbox outbox =
        builder()
            .instantiator(Instantiator.using(clazz -> new JooqWorker(txManager)))
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
                outbox.schedule(JooqWorker.class).process(1);

                txManager.inTransactionThrows(tx2 -> outbox.schedule(JooqWorker.class).process(2));

                // Neither should be fired - the second job is in a nested transaction
                CompletableFuture.allOf(
                        runAsync(
                            () -> uncheck(() -> assertFalse(latch1.await(2, TimeUnit.SECONDS)))),
                        runAsync(
                            () -> uncheck(() -> assertFalse(latch2.await(2, TimeUnit.SECONDS)))))
                    .get();
              });

          // Should be fired after commit
          CompletableFuture.allOf(
                  runAsync(() -> uncheck(() -> assertTrue(latch1.await(2, TimeUnit.SECONDS)))),
                  runAsync(() -> uncheck(() -> assertTrue(latch2.await(2, TimeUnit.SECONDS)))))
              .get();
        });

    TestUtils.assertRecordExists(dsl, 1);
    TestUtils.assertRecordExists(dsl, 2);
  }

  @Test
  void testJooqSimpleViaListenerWithThreadContext() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    TransactionOutbox outbox =
        builder()
            .instantiator(Instantiator.using(clazz -> new JooqWorker(txManager)))
            .listener(
                new TransactionOutboxListener() {
                  @Override
                  public void success(TransactionOutboxEntry entry) {
                    latch.countDown();
                  }
                })
            .build();

    cleanDataStore();

    dsl.transaction(
        () -> {
          outbox.schedule(JooqWorker.class).process(1);
          try {
            // Should not be fired until after commit
            assertFalse(latch.await(2, TimeUnit.SECONDS));
          } catch (InterruptedException e) {
            fail("Interrupted");
          }
        });

    // Should be fired after commit
    assertTrue(latch.await(2, TimeUnit.SECONDS));
    TestUtils.assertRecordExists(dsl, 1);
  }

  @Test
  void testJooqNestedViaListener() throws Exception {
    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(1);
    TransactionOutbox outbox =
        builder()
            .instantiator(Instantiator.using(clazz -> new JooqWorker(txManager)))
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
                outbox.schedule(JooqWorker.class).process(1);
                ctx.dsl().transaction(() -> outbox.schedule(JooqWorker.class).process(2));

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

  /**
   * Ensures that given the rollback of an inner transaction, any outbox work scheduled in the inner
   * transaction is rolled back while the outer transaction's works.
   */
  @Test
  void testJooqNestedWithInnerFailure() throws Exception {
    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(1);
    TransactionOutbox outbox =
        builder()
            .instantiator(Instantiator.using(clazz -> new JooqWorker(txManager)))
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
                outbox.schedule(JooqWorker.class).process(1);

                assertThrows(
                    UnsupportedOperationException.class,
                    () ->
                        ctx.dsl()
                            .transaction(
                                () -> {
                                  outbox.schedule(JooqWorker.class).process(2);
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
  }

  @SuppressWarnings("EmptyMethod")
  static class JooqWorker {

    private final ThreadLocalJooqTransactionManager transactionManager;

    JooqWorker(ThreadLocalJooqTransactionManager transactionManager) {
      this.transactionManager = transactionManager;
    }

    @SuppressWarnings("SameParameterValue")
    void process(int i) {
      TestUtils.writeRecord(transactionManager, i);
    }
  }
}
