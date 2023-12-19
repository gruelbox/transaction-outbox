package com.gruelbox.transactionoutbox.jooq.acceptance;

import static com.gruelbox.transactionoutbox.testing.TestUtils.uncheck;
import static java.util.concurrent.CompletableFuture.runAsync;
import static org.junit.jupiter.api.Assertions.*;

import com.gruelbox.transactionoutbox.*;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.DataSourceConnectionProvider;
import org.jooq.impl.DefaultConfiguration;
import org.jooq.impl.ThreadLocalTransactionProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
abstract class AbstractJooqAcceptanceThreadLocalTest extends AbstractJooqAcceptanceTest {
  private ThreadLocalContextTransactionManager txm;

  @Override
  protected final ThreadLocalContextTransactionManager txManager() {
    return txm;
  }

  protected SQLDialect jooqDialect() {
    return SQLDialect.H2;
  }

  @BeforeEach
  final void beforeEach() {
    DataSourceConnectionProvider connectionProvider = new DataSourceConnectionProvider(dataSource);
    DefaultConfiguration configuration = new DefaultConfiguration();
    configuration.setConnectionProvider(connectionProvider);
    configuration.setSQLDialect(jooqDialect());
    configuration.setTransactionProvider(
        new ThreadLocalTransactionProvider(connectionProvider, true));
    JooqTransactionListener listener = JooqTransactionManager.createListener();
    configuration.set(listener);
    dsl = DSL.using(configuration);
    txm = JooqTransactionManager.create(dsl, listener);
    JooqTestUtils.createTestTable(dsl);
  }

  @Test
  @Override
  void testNestedDirectInvocation() throws Exception {
    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(1);
    ThreadLocalContextTransactionManager transactionManager = txManager();
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(Persistor.forDialect(connectionDetails().dialect()))
            .instantiator(Instantiator.using(clazz -> new Worker(transactionManager)))
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

    clearOutbox();

    withRunningFlusher(
        outbox,
        () -> {
          transactionManager.inTransactionThrows(
              tx1 -> {
                outbox.schedule(Worker.class).process(1);

                transactionManager.inTransactionThrows(
                    tx2 -> outbox.schedule(Worker.class).process(2));

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

    JooqTestUtils.assertRecordExists(dsl, 1);
    JooqTestUtils.assertRecordExists(dsl, 2);
  }

  @Test
  @Override
  void testNestedViaListener() throws Exception {
    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(1);
    ThreadLocalContextTransactionManager transactionManager = txManager();
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(Persistor.forDialect(connectionDetails().dialect()))
            .instantiator(Instantiator.using(clazz -> new Worker(transactionManager)))
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

    clearOutbox();

    withRunningFlusher(
        outbox,
        () -> {
          dsl.transaction(
              ctx -> {
                outbox.schedule(Worker.class).process(1);
                ctx.dsl().transaction(() -> outbox.schedule(Worker.class).process(2));

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
    JooqTestUtils.assertRecordExists(dsl, 1);
    JooqTestUtils.assertRecordExists(dsl, 2);
  }

  /**
   * Ensures that given the rollback of an inner transaction, any outbox work scheduled in the inner
   * transaction is rolled back while the outer transaction's works.
   */
  @Test
  @Override
  void testNestedWithInnerFailure() throws Exception {
    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(1);
    ThreadLocalContextTransactionManager transactionManager = txManager();
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(Persistor.forDialect(connectionDetails().dialect()))
            .instantiator(Instantiator.using(clazz -> new Worker(transactionManager)))
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

    clearOutbox();

    withRunningFlusher(
        outbox,
        () -> {
          dsl.transaction(
              ctx -> {
                outbox.schedule(Worker.class).process(1);

                assertThrows(
                    UnsupportedOperationException.class,
                    () ->
                        ctx.dsl()
                            .transaction(
                                () -> {
                                  outbox.schedule(Worker.class).process(2);
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
  static class Worker {

    private final ThreadLocalContextTransactionManager transactionManager;

    Worker(ThreadLocalContextTransactionManager transactionManager) {
      this.transactionManager = transactionManager;
    }

    @SuppressWarnings("SameParameterValue")
    void process(int i) {
      JooqTestUtils.writeRecord(transactionManager, i);
    }
  }
}
