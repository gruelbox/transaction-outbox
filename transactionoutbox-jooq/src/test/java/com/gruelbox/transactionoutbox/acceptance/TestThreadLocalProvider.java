package com.gruelbox.transactionoutbox.acceptance;

import static com.gruelbox.transactionoutbox.acceptance.TestUtils.uncheck;
import static java.util.concurrent.CompletableFuture.runAsync;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.gruelbox.transactionoutbox.Instantiator;
import com.gruelbox.transactionoutbox.JooqTransaction;
import com.gruelbox.transactionoutbox.JooqTransactionListener;
import com.gruelbox.transactionoutbox.JooqTransactionManager;
import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.Submitter;
import com.gruelbox.transactionoutbox.ThreadLocalJooqTransactionManager;
import com.gruelbox.transactionoutbox.ThrowingRunnable;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.TransactionOutboxListener;
import com.gruelbox.transactionoutbox.sql.Dialect;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.hamcrest.MatcherAssert;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.DataSourceConnectionProvider;
import org.jooq.impl.DefaultConfiguration;
import org.jooq.impl.ThreadLocalTransactionProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
class TestThreadLocalProvider {

  private final ExecutorService unreliablePool =
      new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(16));

  private HikariDataSource dataSource;
  private ThreadLocalJooqTransactionManager transactionManager;
  private DSLContext dsl;

  @BeforeEach
  void beforeEach() {
    dataSource = pooledDataSource();
    transactionManager = createTransactionManager();
    TestUtils.createTestTable(dsl);
  }

  @AfterEach
  void afterEach() {
    dsl.close();
    dataSource.close();
  }

  private HikariDataSource pooledDataSource() {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(
        "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;DEFAULT_LOCK_TIMEOUT=2000;LOB_TIMEOUT=2000;MV_STORE=TRUE");
    config.setUsername("test");
    config.setPassword("test");
    config.addDataSourceProperty("cachePrepStmts", "true");
    return new HikariDataSource(config);
  }

  private ThreadLocalJooqTransactionManager createTransactionManager() {
    DataSourceConnectionProvider connectionProvider = new DataSourceConnectionProvider(dataSource);
    DefaultConfiguration configuration = new DefaultConfiguration();
    configuration.setConnectionProvider(connectionProvider);
    configuration.setSQLDialect(SQLDialect.H2);
    configuration.setTransactionProvider(
        new ThreadLocalTransactionProvider(connectionProvider, true));
    JooqTransactionListener listener = JooqTransactionManager.createListener();
    configuration.set(listener);
    dsl = DSL.using(configuration);
    return JooqTransactionManager.create(dsl, listener);
  }

  @Test
  void testSimpleDirectInvocationWithThreadContext() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(Persistor.forDialect(Dialect.H2))
            .instantiator(Instantiator.using(clazz -> new Worker(transactionManager)))
            .listener(
                new TransactionOutboxListener() {
                  @Override
                  public void success(TransactionOutboxEntry entry) {
                    latch.countDown();
                  }
                })
            .build();

    clearOutbox(transactionManager);

    transactionManager.inTransaction(
        () -> {
          outbox.schedule(Worker.class).process(1);
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
  void testNestedDirectInvocation() throws Exception {
    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(1);
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(Persistor.forDialect(Dialect.H2))
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

    clearOutbox(transactionManager);

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

    TestUtils.assertRecordExists(dsl, 1);
    TestUtils.assertRecordExists(dsl, 2);
  }

  @Test
  void testSimpleViaListenerWithThreadContext() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .instantiator(Instantiator.using(clazz -> new Worker(transactionManager)))
            .persistor(Persistor.forDialect(Dialect.H2))
            .listener(
                new TransactionOutboxListener() {
                  @Override
                  public void success(TransactionOutboxEntry entry) {
                    latch.countDown();
                  }
                })
            .build();

    clearOutbox(transactionManager);

    dsl.transaction(
        () -> {
          outbox.schedule(Worker.class).process(1);
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
  void testNestedViaListener() throws Exception {
    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(1);
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(Persistor.forDialect(Dialect.H2))
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

    clearOutbox(transactionManager);

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
    TestUtils.assertRecordExists(dsl, 1);
    TestUtils.assertRecordExists(dsl, 2);
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
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(Persistor.forDialect(Dialect.H2))
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

    clearOutbox(transactionManager);

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

  @Test
  void retryBehaviour() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger attempts = new AtomicInteger();
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(Persistor.forDialect(Dialect.H2))
            .instantiator(new FailingInstantiator())
            .submitter(Submitter.withExecutor(unreliablePool))
            .attemptFrequency(Duration.ofSeconds(1))
            .listener(
                new TransactionOutboxListener() {
                  @Override
                  public void success(TransactionOutboxEntry entry) {
                    latch.countDown();
                  }
                })
            .build();

    clearOutbox(transactionManager);

    withRunningFlusher(
        outbox,
        () -> {
          transactionManager.inTransaction(() -> outbox.schedule(InterfaceWorker.class).process(3));
          assertTrue(latch.await(15, TimeUnit.SECONDS));
        });
  }

  @Test
  void highVolumeUnreliable() throws Exception {
    int count = 10;

    CountDownLatch latch = new CountDownLatch(count * 10);
    ConcurrentHashMap<Integer, Integer> results = new ConcurrentHashMap<>();
    ConcurrentHashMap<Integer, Integer> duplicates = new ConcurrentHashMap<>();

    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(Persistor.forDialect(Dialect.H2))
            .instantiator(new FailingInstantiator())
            .submitter(Submitter.withExecutor(unreliablePool))
            .attemptFrequency(Duration.ofSeconds(1))
            .flushBatchSize(1000)
            .listener(
                new TransactionOutboxListener() {
                  @Override
                  public void success(TransactionOutboxEntry entry) {
                    Integer i = (Integer) entry.getInvocation().getArgs()[0];
                    if (results.putIfAbsent(i, i) != null) {
                      duplicates.put(i, i);
                    }
                    latch.countDown();
                  }
                })
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
          assertTrue(latch.await(30, TimeUnit.SECONDS));
        });

    MatcherAssert.assertThat(
        "Should never get duplicates running to full completion", duplicates.keySet(), empty());
    MatcherAssert.assertThat(
        "Only got: " + results.keySet(),
        results.keySet(),
        containsInAnyOrder(IntStream.range(0, count * 10).boxed().toArray()));
  }

  private void clearOutbox(ThreadLocalJooqTransactionManager transactionManager) {
    TestUtils.runSql(transactionManager, "DELETE FROM TXNO_OUTBOX");
  }

  private void withRunningFlusher(TransactionOutbox outbox, ThrowingRunnable runnable)
      throws Exception {
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    try {
      scheduler.scheduleAtFixedRate(outbox::flush, 500, 500, TimeUnit.MILLISECONDS);
      runnable.run();
    } finally {
      scheduler.shutdown();
      assertTrue(scheduler.awaitTermination(20, TimeUnit.SECONDS));
    }
  }

  interface InterfaceWorker {

    void process(int i);
  }

  @SuppressWarnings("EmptyMethod")
  static class Worker {

    private final ThreadLocalJooqTransactionManager transactionManager;

    Worker(ThreadLocalJooqTransactionManager transactionManager) {
      this.transactionManager = transactionManager;
    }

    @SuppressWarnings("SameParameterValue")
    void process(int i) {
      TestUtils.writeRecord(transactionManager, i);
    }

    void process(int i, JooqTransaction transaction) {
      TestUtils.writeRecord(transaction, i);
    }

    void process(int i, Configuration configuration) {
      TestUtils.writeRecord(configuration, i);
    }
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
