package com.gruelbox.transactionoutbox.acceptance;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.ea.async.Async;
import com.gruelbox.transactionoutbox.AlreadyScheduledException;
import com.gruelbox.transactionoutbox.Submitter;
import com.gruelbox.transactionoutbox.ThrowingRunnable;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.TransactionOutboxListener;
import com.gruelbox.transactionoutbox.Utils;
import com.gruelbox.transactionoutbox.jdbc.JdbcPersistor;
import com.gruelbox.transactionoutbox.jdbc.SimpleTransactionManager;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@Slf4j
abstract class AbstractAcceptanceTest {

  static {
    Async.init();
  }

  private final ExecutorService unreliablePool =
      new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(16));

  protected abstract ConnectionDetails connectionDetails();

  @Test
  final void testCustomInstantiator() throws InterruptedException {

    SimpleTransactionManager transactionManager = simpleTxnManager();

    CountDownLatch latch = new CountDownLatch(1);
    CountDownLatch chainedLatch = new CountDownLatch(1);
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .instantiator(new LoggingInstantiator())
            .listener(
                new LatchListener(latch)
                    .andThen(
                        new TransactionOutboxListener() {
                          @Override
                          public void success(TransactionOutboxEntry entry) {
                            chainedLatch.countDown();
                          }
                        }))
            .persistor(JdbcPersistor.forDialect(connectionDetails().dialect()))
            .build();

    clearOutbox();

    transactionManager.inTransaction(
        () -> {
          outbox.schedule(InterfaceProcessor.class).process(3, "Whee");
          try {
            // Should not be fired until after commit
            assertFalse(latch.await(2, TimeUnit.SECONDS));
          } catch (InterruptedException e) {
            fail("Interrupted");
          }
        });

    // Should be fired after commit
    assertTrue(chainedLatch.await(2, TimeUnit.SECONDS));
    assertTrue(latch.await(1, TimeUnit.SECONDS));
  }

  @Test
  final void testCustomInstantiatorAsync() {

    SimpleTransactionManager transactionManager = simpleTxnManager();

    CountDownLatch latch = new CountDownLatch(1);
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .instantiator(new LoggingInstantiator())
            .listener(new LatchListener(latch))
            .persistor(JdbcPersistor.forDialect(connectionDetails().dialect()))
            .build();

    clearOutbox();

    boolean success =
        Utils.join(
            transactionManager
                .transactionally(
                    tx ->
                        outbox
                            .schedule(InterfaceProcessor.class)
                            .processAsync(3, "Whee")
                            .thenRun(
                                () -> {
                                  try {
                                    // Should not be fired until after commit
                                    assertFalse(latch.await(2, TimeUnit.SECONDS));
                                  } catch (InterruptedException e) {
                                    fail("Interrupted");
                                  }
                                }))
                .thenApply(
                    __ -> {
                      try {
                        return latch.await(1, TimeUnit.SECONDS);
                      } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                      }
                    }));
    assertTrue(success);
  }

  @Test
  void testDuplicateRequests() {

    SimpleTransactionManager transactionManager = simpleTxnManager();

    List<String> ids = new ArrayList<>();
    AtomicReference<Clock> clockProvider = new AtomicReference<>(Clock.systemDefaultZone());

    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .listener(
                new TransactionOutboxListener() {
                  @Override
                  public void success(TransactionOutboxEntry entry) {
                    ids.add((String) entry.getInvocation().getArgs()[0]);
                  }
                })
            .submitter(Submitter.withExecutor(Runnable::run))
            .persistor(JdbcPersistor.forDialect(connectionDetails().dialect()))
            .retentionThreshold(Duration.ofDays(2))
            .clockProvider(clockProvider::get)
            .build();

    clearOutbox();

    // Schedule some work
    transactionManager.inTransaction(
        () ->
            outbox
                .with()
                .uniqueRequestId("context-clientkey1")
                .schedule(ClassProcessor.class)
                .process("1"));

    // Make sure we can schedule more work with a different client key
    transactionManager.inTransaction(
        () ->
            outbox
                .with()
                .uniqueRequestId("context-clientkey2")
                .schedule(ClassProcessor.class)
                .process("2"));

    // Make sure we can't repeat the same work
    transactionManager.inTransaction(
        () ->
            Assertions.assertThrows(
                AlreadyScheduledException.class,
                () ->
                    outbox
                        .with()
                        .uniqueRequestId("context-clientkey1")
                        .schedule(ClassProcessor.class)
                        .process("3")));

    // Run the clock forward to just under the retention threshold
    clockProvider.set(
        Clock.fixed(
            clockProvider.get().instant().plus(Duration.ofDays(2)).minusSeconds(60),
            clockProvider.get().getZone()));
    outbox.flush();

    // Make sure we can schedule more work with a different client key
    transactionManager.inTransaction(
        () ->
            outbox
                .with()
                .uniqueRequestId("context-clientkey4")
                .schedule(ClassProcessor.class)
                .process("4"));

    // Make sure we still can't repeat the same work
    transactionManager.inTransaction(
        () ->
            Assertions.assertThrows(
                AlreadyScheduledException.class,
                () ->
                    outbox
                        .with()
                        .uniqueRequestId("context-clientkey1")
                        .schedule(ClassProcessor.class)
                        .process("5")));

    // Run the clock over the threshold
    clockProvider.set(
        Clock.fixed(clockProvider.get().instant().plusSeconds(120), clockProvider.get().getZone()));
    outbox.flush();

    // We should now be able to add the work
    transactionManager.inTransaction(
        () ->
            outbox
                .with()
                .uniqueRequestId("context-clientkey1")
                .schedule(ClassProcessor.class)
                .process("6"));

    assertThat(ids, containsInAnyOrder("1", "2", "4", "6"));
  }

  /**
   * Uses a simple data source transaction manager and attempts to fire a concrete class via
   * reflection.
   */
  @Test
  final void testDataSourceConnectionProviderReflectionInstantiatorConcreteClass()
      throws InterruptedException {
    try (HikariDataSource ds = pooledDataSource()) {

      CountDownLatch latch = new CountDownLatch(1);

      SimpleTransactionManager transactionManager = SimpleTransactionManager.fromDataSource(ds);
      TransactionOutbox outbox =
          TransactionOutbox.builder()
              .transactionManager(transactionManager)
              .persistor(JdbcPersistor.forDialect(connectionDetails().dialect()))
              .listener(new LatchListener(latch))
              .build();

      clearOutbox();
      ClassProcessor.PROCESSED.clear();
      String myId = UUID.randomUUID().toString();

      transactionManager.inTransaction(() -> outbox.schedule(ClassProcessor.class).process(myId));

      assertTrue(latch.await(2, TimeUnit.SECONDS));
      assertEquals(List.of(myId), ClassProcessor.PROCESSED);
    }
  }

  /**
   * Runs a piece of work which will fail several times before working successfully. Ensures that
   * the work runs eventually.
   */
  @Test
  final void testRetryBehaviour() throws Exception {
    SimpleTransactionManager transactionManager = simpleTxnManager();
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger attempts = new AtomicInteger();
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(JdbcPersistor.forDialect(connectionDetails().dialect()))
            .instantiator(new FailingInstantiator(attempts))
            .submitter(Submitter.withExecutor(unreliablePool))
            .attemptFrequency(Duration.ofMillis(500))
            .listener(new LatchListener(latch))
            .build();

    clearOutbox();

    withRunningFlusher(
        outbox,
        () -> {
          transactionManager.inTransaction(
              () -> outbox.schedule(InterfaceProcessor.class).process(3, "Whee"));
          assertTrue(latch.await(15, TimeUnit.SECONDS));
        });
  }

  /**
   * Runs a piece of work which will fail enough times to be blacklisted but will then pass when
   * re-whitelisted.
   */
  @Test
  final void testBlackAndWhitelist() throws Exception {
    SimpleTransactionManager transactionManager = simpleTxnManager();
    CountDownLatch successLatch = new CountDownLatch(1);
    CountDownLatch blacklistLatch = new CountDownLatch(1);
    LatchListener latchListener = new LatchListener(successLatch, blacklistLatch);
    AtomicInteger attempts = new AtomicInteger();
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(JdbcPersistor.forDialect(connectionDetails().dialect()))
            .instantiator(new FailingInstantiator(attempts))
            .attemptFrequency(Duration.ofMillis(500))
            .listener(latchListener)
            .blacklistAfterAttempts(2)
            .build();

    clearOutbox();

    withRunningFlusher(
        outbox,
        () -> {
          transactionManager.inTransaction(
              () -> outbox.schedule(InterfaceProcessor.class).process(3, "Whee"));
          assertTrue(blacklistLatch.await(3, TimeUnit.SECONDS));
          assertTrue(
              transactionManager.inTransactionReturns(
                  tx -> outbox.whitelist(latchListener.getBlacklisted().getId())));
          assertTrue(successLatch.await(3, TimeUnit.SECONDS));
        });
  }

  @Test
  final void testBlackAndWhitelistAsync() throws Exception {
    SimpleTransactionManager transactionManager = simpleTxnManager();
    CountDownLatch successLatch = new CountDownLatch(1);
    CountDownLatch blacklistLatch = new CountDownLatch(1);
    LatchListener latchListener = new LatchListener(successLatch, blacklistLatch);
    AtomicInteger attempts = new AtomicInteger();
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(JdbcPersistor.forDialect(connectionDetails().dialect()))
            .instantiator(new FailingInstantiator(attempts))
            .attemptFrequency(Duration.ofMillis(500))
            .listener(latchListener)
            .blacklistAfterAttempts(2)
            .build();

    clearOutbox();

    withRunningFlusher(
        outbox,
        () ->
            Utils.join(
                transactionManager
                    .transactionally(
                        tx -> outbox.schedule(InterfaceProcessor.class).processAsync(3, "Whee"))
                    .thenRun(
                        () -> {
                          try {
                            assertTrue(blacklistLatch.await(3, TimeUnit.SECONDS));
                          } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                          }
                        })
                    .thenCompose(
                        __ ->
                            transactionManager.transactionally(
                                tx ->
                                    outbox.whitelistAsync(latchListener.getBlacklisted().getId())))
                    .thenRun(
                        () -> {
                          try {
                            assertTrue(successLatch.await(3, TimeUnit.SECONDS));
                          } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                          }
                        })));
  }

  /** Hammers high-volume, frequently failing tasks to ensure that they all get run. */
  @Test
  final void testHighVolumeUnreliable() throws Exception {
    int count = 10;

    SimpleTransactionManager transactionManager = simpleTxnManager();
    CountDownLatch latch = new CountDownLatch(count * 10);
    ConcurrentHashMap<Integer, Integer> results = new ConcurrentHashMap<>();
    ConcurrentHashMap<Integer, Integer> duplicates = new ConcurrentHashMap<>();

    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(JdbcPersistor.forDialect(connectionDetails().dialect()))
            .instantiator(new RandomFailingInstantiator())
            .submitter(Submitter.withExecutor(unreliablePool))
            .attemptFrequency(Duration.ofMillis(500))
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
                              outbox.schedule(InterfaceProcessor.class).process(i * 10 + j, "Whee");
                            }
                          }));
          assertTrue("Latch not opened in time", latch.await(30, TimeUnit.SECONDS));
        });

    assertThat(
        "Should never get duplicates running to full completion", duplicates.keySet(), empty());
    assertThat(
        "Only got: " + results.keySet(),
        results.keySet(),
        containsInAnyOrder(IntStream.range(0, count * 10).boxed().toArray()));
  }

  /** Hammers high-volume, frequently failing tasks to ensure that they all get run. */
  @Test
  final void testHighVolumeUnreliableAsync() throws Exception {
    int count = 10;

    SimpleTransactionManager transactionManager = simpleTxnManager();
    CountDownLatch latch = new CountDownLatch(count * 10);
    ConcurrentHashMap<Integer, Integer> results = new ConcurrentHashMap<>();
    ConcurrentHashMap<Integer, Integer> duplicates = new ConcurrentHashMap<>();

    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(JdbcPersistor.forDialect(connectionDetails().dialect()))
            .instantiator(new RandomFailingInstantiator())
            .submitter(Submitter.withExecutor(unreliablePool))
            .attemptFrequency(Duration.ofMillis(500))
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

    clearOutbox();

    withRunningFlusher(
        outbox,
        () -> {
          IntStream.range(0, count)
              .parallel()
              .forEach(
                  i ->
                      transactionManager.transactionally(
                          tx ->
                              CompletableFuture.allOf(
                                  IntStream.range(0, 10)
                                      .mapToObj(
                                          j ->
                                              outbox
                                                  .schedule(InterfaceProcessor.class)
                                                  .processAsync(i * 10 + j, "Whee"))
                                      .toArray(CompletableFuture[]::new))));
          assertTrue("Latch not opened in time", latch.await(30, TimeUnit.SECONDS));
        });

    assertThat(
        "Should never get duplicates running to full completion", duplicates.keySet(), empty());
    assertThat(
        "Only got: " + results.keySet(),
        results.keySet(),
        containsInAnyOrder(IntStream.range(0, count * 10).boxed().toArray()));
  }

  private SimpleTransactionManager simpleTxnManager() {
    return SimpleTransactionManager.fromConnectionDetails(
        connectionDetails().driverClassName(),
        connectionDetails().url(),
        connectionDetails().user(),
        connectionDetails().password());
  }

  private HikariDataSource pooledDataSource() {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(connectionDetails().url());
    config.setUsername(connectionDetails().user());
    config.setPassword(connectionDetails().password());
    config.addDataSourceProperty("cachePrepStmts", "true");
    return new HikariDataSource(config);
  }

  private void clearOutbox() {
    TestUtils.runSqlViaJdbc(simpleTxnManager(), "DELETE FROM TXNO_OUTBOX");
  }

  private void withRunningFlusher(TransactionOutbox outbox, ThrowingRunnable runnable)
      throws Exception {
    Thread backgroundThread =
        new Thread(
            () -> {
              while (!Thread.interrupted()) {
                try {
                  // Keep flushing work until there's nothing left to flush
                  //noinspection StatementWithEmptyBody
                  while (outbox.flush()) {}
                } catch (Exception e) {
                  log.error("Error flushing transaction outbox. Pausing", e);
                }
                try {
                  Thread.sleep(250);
                } catch (InterruptedException e) {
                  break;
                }
              }
            });
    backgroundThread.start();
    try {
      runnable.run();
    } finally {
      backgroundThread.interrupt();
      backgroundThread.join();
    }
  }
}
