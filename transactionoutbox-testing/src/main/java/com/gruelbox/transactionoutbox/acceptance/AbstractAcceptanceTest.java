package com.gruelbox.transactionoutbox.acceptance;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.delayedExecutor;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.gruelbox.transactionoutbox.AlreadyScheduledException;
import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.SchedulerProxyFactory;
import com.gruelbox.transactionoutbox.Submitter;
import com.gruelbox.transactionoutbox.ThrowingRunnable;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.TransactionOutboxListener;
import com.gruelbox.transactionoutbox.Utils;
import com.gruelbox.transactionoutbox.spi.BaseTransaction;
import com.gruelbox.transactionoutbox.spi.BaseTransactionManager;
import com.gruelbox.transactionoutbox.sql.Dialect;
import java.lang.StackWalker.StackFrame;
import java.time.Clock;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

/**
 * Generalised, non-blocking acceptance tests which all combinations of transaction manager,
 * persistor and dialect need to conform, regardless of implementation.
 *
 * <p>There's a SQL assumption built in here
 *
 * @param <CN> The connection type.
 * @param <TX> The transaction type.
 * @param <TM> The transaction manager type
 */
@Slf4j
public abstract class AbstractAcceptanceTest<
    CN, TX extends BaseTransaction<CN>, TM extends BaseTransactionManager<CN, ? extends TX>> {

  protected final ExecutorService unreliablePool =
      new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(16));

  protected abstract Dialect dialect();

  protected abstract Persistor<CN, TX> createPersistor();

  protected abstract TM createTxManager();

  @SuppressWarnings("SameParameterValue")
  protected abstract CompletableFuture<Void> scheduleWithTx(
      SchedulerProxyFactory outbox, TX tx, int arg1, String arg2);

  @SuppressWarnings("SameParameterValue")
  protected abstract CompletableFuture<Void> scheduleWithCtx(
      SchedulerProxyFactory outbox, Object context, int arg1, String arg2);

  protected abstract void prepareDataStore();

  protected abstract void cleanDataStore();

  protected abstract CompletableFuture<?> incrementRecordCount(Object txOrContext);

  protected abstract CompletableFuture<Long> countRecords(TX tx);

  private static Persistor<?, ?> staticPersistor;
  private static BaseTransactionManager<?, ?> staticTxManager;
  protected Persistor<CN, TX> persistor;
  protected TM txManager;
  private static final Deque<AutoCloseable> resources = new LinkedList<>();

  @BeforeAll
  static void cleanState() {
    staticPersistor = null;
    staticTxManager = null;
  }

  @AfterAll
  static void cleanup() {
    Utils.safelyClose(resources);
  }

  @SuppressWarnings("unchecked")
  @BeforeEach
  void beforeEach() {
    log.info("Initialising test");
    if (staticPersistor == null) {
      log.info("Creating new transaction manager");
      staticPersistor = createPersistor();
      staticTxManager = createTxManager();
      persistor = (Persistor<CN, TX>) staticPersistor;
      txManager = (TM) staticTxManager;
      prepareDataStore();
    } else {
      log.info("Using existing transaction manager");
      persistor = (Persistor<CN, TX>) staticPersistor;
      txManager = (TM) staticTxManager;
    }
  }

  protected TransactionOutbox.TransactionOutboxBuilder builder() {
    return TransactionOutbox.builder()
        .transactionManager(txManager)
        .logLevelTemporaryFailure(Level.DEBUG)
        .persistor(persistor);
  }

  @Test
  final void testAsyncSimple() {
    CountDownLatch latch = new CountDownLatch(1);
    CountDownLatch chainCompleted = new CountDownLatch(1);
    TransactionOutbox outbox =
        builder()
            .instantiator(new LoggingInstantiator())
            .listener(new LatchListener(latch, Level.INFO))
            .build();

    cleanDataStore();

    Utils.join(
        txManager
            .transactionally(
                tx ->
                    scheduleWithTx(outbox, tx, 3, "Whee").thenRunAsync(() -> assertNotFired(latch)))
            .thenRunAsync(() -> assertFired(latch))
            .thenRun(chainCompleted::countDown));

    assertFired(chainCompleted);
  }

  @Test
  final void testAsyncSimpleWithDatabaseAccessViaTransaction() {
    CountDownLatch latch = new CountDownLatch(1);
    CountDownLatch chainCompleted = new CountDownLatch(1);
    TransactionOutbox outbox =
        builder()
            .instantiator(new InsertingInstantiator(this::incrementRecordCount))
            .listener(new LatchListener(latch))
            .build();

    cleanDataStore();

    Utils.join(
        txManager
            .transactionally(
                tx ->
                    scheduleWithTx(outbox, tx, 3, "Whee")
                        .thenRunAsync(() -> assertNotFired(latch))
                        .thenCompose(__ -> countRecords(tx))
                        .thenApply(
                            count -> {
                              assertEquals(0, count);
                              return count;
                            }))
            .thenRunAsync(() -> assertFired(latch))
            .thenRun(chainCompleted::countDown));

    assertFired(chainCompleted);

    Long rowCount = Utils.join(txManager.transactionally(this::countRecords));
    assertEquals(1, rowCount);
  }

  @Test
  final void testAsyncSimpleWithDatabaseAccessViaContext() {
    CountDownLatch latch = new CountDownLatch(1);
    CountDownLatch chainCompleted = new CountDownLatch(1);
    TransactionOutbox outbox =
        builder()
            .instantiator(new InsertingInstantiator(this::incrementRecordCount))
            .listener(new LatchListener(latch))
            .build();

    cleanDataStore();

    try {
      Utils.join(
          txManager
              .transactionally(
                  tx ->
                      scheduleWithCtx(outbox, tx.context(), 3, "Whee")
                          .thenRunAsync(() -> assertNotFired(latch))
                          .thenCompose(__ -> countRecords(tx))
                          .thenApplyAsync(
                              count -> {
                                assertEquals(0, count);
                                return count;
                              }))
              .thenRunAsync(() -> assertFired(latch))
              .thenRun(chainCompleted::countDown));
    } catch (UnsupportedOperationException e) {
      notTestingContextInjection();
    } catch (Exception e) {
      if (e.getCause() instanceof UnsupportedOperationException) {
        notTestingContextInjection();
      } else {
        throw e;
      }
    }

    assertFired(chainCompleted);

    Long rowCount = Utils.join(txManager.transactionally(this::countRecords));
    assertEquals(1, rowCount);
  }

  @Test
  final void testAsyncBlackAndWhitelistViaTxn() throws Exception {
    CountDownLatch successLatch = new CountDownLatch(1);
    CountDownLatch blacklistLatch = new CountDownLatch(1);
    LatchListener latchListener = new LatchListener(successLatch, blacklistLatch);
    TransactionOutbox outbox =
        builder()
            .instantiator(new FailingInstantiator(2))
            .attemptFrequency(Duration.ofMillis(500))
            .listener(latchListener)
            .blacklistAfterAttempts(2)
            .build();

    cleanDataStore();

    withRunningFlusher(
        outbox,
        () ->
            Utils.join(
                txManager
                    .transactionally(tx -> scheduleWithTx(outbox, tx, 3, "Whee"))
                    .thenRunAsync(() -> assertFired(blacklistLatch))
                    .thenCompose(
                        __ ->
                            txManager.transactionally(
                                tx ->
                                    outbox.whitelistAsync(
                                        latchListener.getBlacklisted().getId(), tx)))
                    .thenRunAsync(() -> assertFired(successLatch))));
  }

  @Test
  final void testAsyncBlackAndWhitelistViaContext() throws Exception {
    CountDownLatch successLatch = new CountDownLatch(1);
    CountDownLatch blacklistLatch = new CountDownLatch(1);
    LatchListener latchListener = new LatchListener(successLatch, blacklistLatch);
    TransactionOutbox outbox =
        builder()
            .instantiator(new FailingInstantiator(2, false))
            .attemptFrequency(Duration.ofMillis(500))
            .listener(latchListener)
            .blacklistAfterAttempts(2)
            .build();

    cleanDataStore();

    withRunningFlusher(
        outbox,
        () -> {
          try {
            Utils.join(
                txManager
                    .transactionally(tx -> scheduleWithCtx(outbox, tx.context(), 3, "Whee"))
                    .thenRunAsync(() -> assertFired(blacklistLatch))
                    .thenCompose(
                        __ ->
                            txManager.transactionally(
                                tx ->
                                    outbox.whitelistAsync(
                                        latchListener.getBlacklisted().getId(),
                                        (Object) tx.context())))
                    .thenRunAsync(() -> assertFired(successLatch)));
          } catch (UnsupportedOperationException e) {
            notTestingContextInjection();
          } catch (Exception e) {
            if (e.getCause() instanceof UnsupportedOperationException) {
              notTestingContextInjection();
            } else {
              throw e;
            }
          }
        });
  }

  @Test
  final void testAsyncDuplicateRequests() {

    CountDownLatch priorWorkClear = new CountDownLatch(3);
    CountDownLatch latch = new CountDownLatch(4);
    List<Integer> ids = new ArrayList<>();
    AtomicReference<Clock> clockProvider = new AtomicReference<>(Clock.systemDefaultZone());
    TransactionOutbox outbox =
        builder()
            .listener(
                new TransactionOutboxListener() {
                  @Override
                  public void success(TransactionOutboxEntry entry) {
                    ids.add((Integer) entry.getInvocation().getArgs()[0]);
                    priorWorkClear.countDown();
                    latch.countDown();
                  }
                })
            .instantiator(new LoggingInstantiator())
            .retentionThreshold(Duration.ofDays(2))
            .clockProvider(clockProvider::get)
            .build();

    cleanDataStore();

    // Schedule some work
    txManager
        .transactionally(
            tx -> scheduleWithTx(outbox.with().uniqueRequestId("context-clientkey1"), tx, 1, "Foo"))
        .join();

    // Make sure we can schedule more work with a different client key
    txManager
        .transactionally(
            tx -> scheduleWithTx(outbox.with().uniqueRequestId("context-clientkey2"), tx, 2, "Foo"))
        .join();

    // Make sure we can't repeat the same work
    assertThrows(
        AlreadyScheduledException.class,
        () ->
            Utils.join(
                txManager.transactionally(
                    tx ->
                        scheduleWithTx(
                            outbox.with().uniqueRequestId("context-clientkey1"), tx, 3, "Bar"))));

    // Run the clock forward to just under the retention threshold
    clockProvider.set(
        Clock.fixed(
            clockProvider.get().instant().plus(Duration.ofDays(2)).minusSeconds(60),
            clockProvider.get().getZone()));
    outbox.flush();

    // Make sure we can schedule more work with a different client key
    txManager
        .transactionally(
            tx ->
                scheduleWithTx(
                    outbox.with().uniqueRequestId("context-clientkey4"), tx, 4, "Whoohoo"))
        .join();

    // Make sure we still can't repeat the same work
    assertThrows(
        AlreadyScheduledException.class,
        () ->
            Utils.join(
                txManager.transactionally(
                    tx ->
                        scheduleWithTx(
                            outbox.with().uniqueRequestId("context-clientkey1"),
                            tx,
                            5,
                            "Whooops"))));

    assertFired(priorWorkClear);

    // Run the clock over the threshold
    clockProvider.set(
        Clock.fixed(clockProvider.get().instant().plusSeconds(120), clockProvider.get().getZone()));
    outbox.flush();

    // We should now be able to add the work
    txManager
        .transactionally(
            tx ->
                scheduleWithTx(outbox.with().uniqueRequestId("context-clientkey1"), tx, 6, "Yes!"))
        .join();

    assertFired(latch);

    assertThat(ids, containsInAnyOrder(1, 2, 4, 6));
  }

  /** Hammers high-volume, frequently failing tasks to ensure that they all get run. */
  @Test
  final void testAsyncHighVolumeUnreliable() throws Exception {
    int count = 50;

    CountDownLatch latch = new CountDownLatch(count * 10);
    ConcurrentHashMap<Integer, Integer> results = new ConcurrentHashMap<>();
    ConcurrentHashMap<Integer, Integer> duplicates = new ConcurrentHashMap<>();

    TransactionOutbox outbox =
        builder()
            .instantiator(new RandomFailingInstantiator(false))
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

    cleanDataStore();

    withRunningFlusher(
        outbox,
        () -> {
          IntStream.range(0, count)
              .parallel()
              .forEach(
                  i ->
                      txManager.transactionally(
                          tx ->
                              CompletableFuture.allOf(
                                  IntStream.range(0, 10)
                                      .mapToObj(j -> scheduleWithTx(outbox, tx, i * 10 + j, "Whee"))
                                      .toArray(CompletableFuture[]::new))));
          assertFired(latch, 60);
        });

    assertThat(
        "Should never get duplicates running to full completion", duplicates.keySet(), empty());
    assertThat(
        "Only got: " + results.keySet(),
        results.keySet(),
        containsInAnyOrder(IntStream.range(0, count * 10).boxed().toArray()));
  }

  /** Ensures that we correctly handle expiry of large numbers of */
  @Test
  final void testAsyncLargeExpiryBatches() {
    int count = 29;
    CountDownLatch latch = new CountDownLatch(count);
    AtomicReference<Clock> clockProvider = new AtomicReference<>(Clock.systemDefaultZone());
    TransactionOutbox outbox =
        builder()
            .instantiator(new LoggingInstantiator())
            .flushBatchSize(10)
            .clockProvider(clockProvider::get)
            .retentionThreshold(Duration.ofDays(1))
            .listener(new LatchListener(latch))
            .build();

    cleanDataStore();

    txManager
        .transactionally(
            tx ->
                CompletableFuture.allOf(
                    IntStream.range(0, count)
                        .mapToObj(
                            i ->
                                scheduleWithTx(
                                    outbox.with().uniqueRequestId("UQ" + i), tx, i, "Whee"))
                        .toArray(CompletableFuture[]::new)))
        .thenRunAsync(() -> assertFired(latch))
        .thenRunAsync(
            () ->
                clockProvider.set(
                    Clock.fixed(
                        clockProvider.get().instant().plus(1, ChronoUnit.DAYS).plusSeconds(60),
                        clockProvider.get().getZone())))
        .thenCompose(__ -> outbox.flushAsync())
        .thenCompose(
            didWork -> {
              assertTrue(didWork);
              return outbox.flushAsync();
            })
        .thenCompose(
            didWork -> {
              assertTrue(didWork);
              return outbox.flushAsync();
            })
        .thenCompose(
            didWork -> {
              assertTrue(didWork);
              return outbox.flushAsync();
            })
        .thenAccept(Assertions::assertFalse)
        .join();
  }

  protected void withRunningFlusher(TransactionOutbox outbox, ThrowingRunnable runnable)
      throws Exception {
    String caller = Utils.traceCaller().map(StackFrame::getMethodName).orElse("<Unknown>");
    AtomicBoolean shutdown = new AtomicBoolean();
    CompletableFuture<Boolean> background = flushRecursive(outbox, caller, shutdown);
    try {
      runnable.run();
      log.info("Test complete");
    } finally {
      log.info("Ordered shutdown of background work");
      shutdown.set(true);
      background.join();
      log.info("Shutdown complete");
    }
  }

  CompletableFuture<Boolean> flushRecursive(TransactionOutbox outbox, String caller,
      AtomicBoolean shutdown) {
    return outbox.flushAsync()
        .exceptionally(
            e -> {
              log.error("Error in poller for {}", caller, e);
              return false;
            })
        .thenCompose(didWork -> {
          if (shutdown.get()) {
            return completedFuture(false);
          } else if (didWork) {
            return completedFuture(true)
                .thenComposeAsync(__ -> flushRecursive(outbox, caller, shutdown));
          } else {
            return completedFuture(false)
                .thenComposeAsync(__ -> flushRecursive(outbox, caller, shutdown),
                    delayedExecutor(250, TimeUnit.MILLISECONDS));
          }
        });
  }

  protected void assertFired(CountDownLatch latch) {
    assertFired(latch, 6);
  }

  protected void assertFired(CountDownLatch latch, int seconds) {
    try {
      boolean fired = latch.await(seconds, TimeUnit.SECONDS);
      if (fired) {
        log.info("Hit latch");
      } else {
        log.error("Failed to hit latch");
      }
      assertTrue(fired);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  protected void assertNotFired(CountDownLatch latch) {
    try {
      boolean fired = latch.await(2, TimeUnit.SECONDS);
      if (fired) {
        log.error("Hit latch unexpectedly");
      } else {
        log.info("No latch fired as expected");
      }
      assertFalse(fired);
    } catch (InterruptedException e) {
      fail("Interrupted");
    }
  }

  @SuppressWarnings("ConstantConditions")
  private void notTestingContextInjection() {
    log.info("Not testing context injection, not supported by transaction manager");
    Assumptions.assumeTrue(
        false, "Not testing context injection, not supported by transaction manager");
  }

  protected <T extends AutoCloseable> T autoClose(T resource) {
    resources.push(resource);
    return resource;
  }
}
