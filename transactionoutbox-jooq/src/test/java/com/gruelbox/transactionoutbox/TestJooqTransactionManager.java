package com.gruelbox.transactionoutbox;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.hamcrest.MatcherAssert;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestJooqTransactionManager {

  private final ExecutorService unreliablePool =
      new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(16));

  private final TransactionManager transactionManager =
      JooqTransactionManager.builder()
          .parentDsl(
              DSL.using(
                  "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;DEFAULT_LOCK_TIMEOUT=60000;LOB_TIMEOUT=2000;MV_STORE=TRUE",
                  "test",
                  "test"))
          .build();

  @Test
  final void testSimple() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .listener(entry -> latch.countDown())
            .dialect(Dialect.H2)
            .build();

    clearOutbox(transactionManager);

    transactionManager.inTransaction(
        () -> {
          outbox.schedule(Worker.class).process(1);
          try {
            // Should not be fired until after commit
            Assertions.assertFalse(latch.await(2, TimeUnit.SECONDS));
          } catch (InterruptedException e) {
            fail("Interrupted");
          }
        });

    // Should be fired after commit
    Assertions.assertTrue(latch.await(2, TimeUnit.SECONDS));
  }

  @Test
  final void retryBehaviour() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger attempts = new AtomicInteger();
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .instantiator(new FailingInstantiator())
            .executorService(unreliablePool)
            .attemptFrequency(Duration.ofSeconds(1))
            .listener(entry -> latch.countDown())
            .dialect(Dialect.H2)
            .build();

    clearOutbox(transactionManager);

    withRunningFlusher(
        outbox,
        () -> {
          transactionManager.inTransaction(
              () -> outbox.schedule(InterfaceWorker.class).process(3));
          Assertions.assertTrue(latch.await(15, TimeUnit.SECONDS));
        });
  }

  @Test
  final void highVolumeUnreliable() throws Exception {
    int count = 10;

    CountDownLatch latch = new CountDownLatch(count * 10);
    ConcurrentHashMap<Integer, Integer> results = new ConcurrentHashMap<>();
    ConcurrentHashMap<Integer, Integer> duplicates = new ConcurrentHashMap<>();

    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .instantiator(new FailingInstantiator())
            .executorService(unreliablePool)
            .attemptFrequency(Duration.ofSeconds(1))
            .flushBatchSize(1000)
            .listener(
                entry -> {
                  Integer i = (Integer) entry.getInvocation().getArgs()[0];
                  if (results.putIfAbsent(i, i) != null) {
                    duplicates.put(i, i);
                  }
                  latch.countDown();
                })
            .dialect(Dialect.H2)
            .build();

    withRunningFlusher(
        outbox,
        () -> {
          IntStream.range(0, count)
              .parallel()
              .forEach(
                  i ->
                      transactionManager.inTransaction(
                          () -> {
                            for (int j = 0; j < 10; j++) {
                              outbox.schedule(InterfaceWorker.class).process(i * 10 + j);
                            }
                          }));
          Assertions.assertTrue(latch.await(30, TimeUnit.SECONDS));
        });

    MatcherAssert.assertThat(
        "Should never get duplicates running to full completion", duplicates.keySet(), empty());
    MatcherAssert.assertThat(
        "Only got: " + results.keySet(),
        results.keySet(),
        containsInAnyOrder(IntStream.range(0, count * 10).boxed().toArray()));
  }

  private void clearOutbox(TransactionManager transactionManager) {
    TestUtils.runSql(transactionManager, "DELETE FROM TXNO_OUTBOX");
  }

  private void withRunningFlusher(TransactionOutbox outbox, ThrowingRunnable runnable)
      throws Exception {
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    try {
      scheduler.scheduleAtFixedRate(
          () -> {
            if (Thread.interrupted()) {
              return;
            }
            outbox.flush();
          },
          500,
          500,
          TimeUnit.MILLISECONDS);
      runnable.run();
    } finally {
      scheduler.shutdown();
      Assertions.assertTrue(scheduler.awaitTermination(20, TimeUnit.SECONDS));
    }
  }

  static class Worker {
    void process(int i) {
      // No-op
    }
  }

  interface InterfaceWorker {
    void process(int i);
  }

  private static class FailingInstantiator implements Instantiator {

    private final AtomicInteger attempts;

    FailingInstantiator() {
      this.attempts = new AtomicInteger(0);
    }

    @Override
    public String getName(Class<?> clazz) {
      return clazz.getName();
    }

    @Override
    public Object getInstance(String name) {
      return (InterfaceWorker)
          (i) -> {
            if (attempts.incrementAndGet() < 3) {
              throw new RuntimeException("Temporary failure");
            }
          };
    }
  }
}
