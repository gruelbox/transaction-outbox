package com.gruelbox.transactionoutbox.acceptance;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gruelbox.transactionoutbox.AlreadyScheduledException;
import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.Submitter;
import com.gruelbox.transactionoutbox.ThrowingRunnable;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.TransactionOutboxListener;
import com.gruelbox.transactionoutbox.jdbc.JdbcPersistor;
import com.gruelbox.transactionoutbox.jdbc.JdbcTransaction;
import com.gruelbox.transactionoutbox.jdbc.JdbcTransactionManager;
import com.gruelbox.transactionoutbox.sql.Dialect;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

@Slf4j
public abstract class AbstractJdbcAcceptanceTest<
        TX extends JdbcTransaction, TM extends JdbcTransactionManager<TX>>
    extends AbstractSqlAcceptanceTest<Connection, TX, TM> {

  protected abstract JdbcConnectionDetails connectionDetails();

  protected boolean supportsThreadLocalContext() {
    return true;
  }

  @Override
  protected final Dialect dialect() {
    return connectionDetails().dialect();
  }

  @Override
  protected CompletableFuture<?> runSql(Object txOrContext, String sql) {
    Connection connection;
    if (txOrContext instanceof JdbcTransaction) {
      connection = ((JdbcTransaction) txOrContext).connection();
    } else {
      connection = (Connection) txOrContext;
    }
    if (connection == null) {
      throw new IllegalStateException(
          "No connection found in transaction or context: " + txOrContext);
    }
    try (Statement stmt = connection.createStatement()) {
      stmt.execute(sql);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return completedFuture(null);
  }

  @Override
  protected CompletableFuture<Long> readLongValue(TX tx, String sql) {
    try (Statement stmt = tx.connection().createStatement();
        ResultSet rs = stmt.executeQuery(sql)) {
      if (!rs.next()) {
        throw new IllegalStateException("No result");
      }
      return completedFuture(rs.getLong(1));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  protected final Persistor<Connection, TX> createPersistor() {
    return (Persistor) JdbcPersistor.builder().dialect(dialect()).migrationRetries(0).build();
  }

  @Test
  final void testBlockingCustomInstantiator() {

    CountDownLatch latch = new CountDownLatch(1);
    CountDownLatch chainedLatch = new CountDownLatch(1);
    TransactionOutbox outbox =
        builder()
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
            .build();

    cleanDataStore();

    txManager.inTransaction(
        tx -> {
          outbox.schedule(BlockingInterfaceProcessor.class).process(3, "Whee", tx);
          assertNotFired(latch);
        });

    // Should be fired after commit
    assertFired(chainedLatch);
    assertFired(latch);
  }

  @Test
  final void testBlockingDuplicateRequestsThreadLocal() {
    Assumptions.assumeTrue(supportsThreadLocalContext());

    List<String> ids = new ArrayList<>();
    AtomicReference<Clock> clockProvider = new AtomicReference<>(Clock.systemDefaultZone());

    TransactionOutbox outbox =
        builder()
            .listener(
                new TransactionOutboxListener() {
                  @Override
                  public void success(TransactionOutboxEntry entry) {
                    ids.add((String) entry.getInvocation().getArgs()[0]);
                  }
                })
            .submitter(Submitter.withExecutor(Runnable::run))
            .retentionThreshold(Duration.ofDays(2))
            .clockProvider(clockProvider::get)
            .build();

    cleanDataStore();

    // Schedule some work
    txManager.inTransaction(
        () ->
            outbox
                .with()
                .uniqueRequestId("context-clientkey1")
                .schedule(BlockingClassProcessor.class)
                .process("1"));

    // Make sure we can schedule more work with a different client key
    txManager.inTransaction(
        () ->
            outbox
                .with()
                .uniqueRequestId("context-clientkey2")
                .schedule(BlockingClassProcessor.class)
                .process("2"));

    // Make sure we can't repeat the same work
    txManager.inTransaction(
        () ->
            Assertions.assertThrows(
                AlreadyScheduledException.class,
                () ->
                    outbox
                        .with()
                        .uniqueRequestId("context-clientkey1")
                        .schedule(BlockingClassProcessor.class)
                        .process("3")));

    // Run the clock forward to just under the retention threshold
    clockProvider.set(
        Clock.fixed(
            clockProvider.get().instant().plus(Duration.ofDays(2)).minusSeconds(60),
            clockProvider.get().getZone()));
    outbox.flush();

    // Make sure we can schedule more work with a different client key
    txManager.inTransaction(
        () ->
            outbox
                .with()
                .uniqueRequestId("context-clientkey4")
                .schedule(BlockingClassProcessor.class)
                .process("4"));

    // Make sure we still can't repeat the same work
    txManager.inTransaction(
        () ->
            Assertions.assertThrows(
                AlreadyScheduledException.class,
                () ->
                    outbox
                        .with()
                        .uniqueRequestId("context-clientkey1")
                        .schedule(BlockingClassProcessor.class)
                        .process("5")));

    // Run the clock over the threshold
    clockProvider.set(
        Clock.fixed(clockProvider.get().instant().plusSeconds(120), clockProvider.get().getZone()));
    outbox.flush();

    // We should now be able to add the work
    txManager.inTransaction(
        () ->
            outbox
                .with()
                .uniqueRequestId("context-clientkey1")
                .schedule(BlockingClassProcessor.class)
                .process("6"));

    assertThat(ids, containsInAnyOrder("1", "2", "4", "6"));
  }

  @Test
  final void testBlockingDuplicateRequestsDirectTx() {

    List<String> ids = new ArrayList<>();
    AtomicReference<Clock> clockProvider = new AtomicReference<>(Clock.systemDefaultZone());

    TransactionOutbox outbox =
        builder()
            .listener(
                new TransactionOutboxListener() {
                  @Override
                  public void success(TransactionOutboxEntry entry) {
                    ids.add((String) entry.getInvocation().getArgs()[0]);
                  }
                })
            .submitter(Submitter.withExecutor(Runnable::run))
            .retentionThreshold(Duration.ofDays(2))
            .clockProvider(clockProvider::get)
            .build();

    cleanDataStore();

    // Schedule some work
    txManager.inTransaction(
        tx ->
            outbox
                .with()
                .uniqueRequestId("context-clientkey1")
                .schedule(BlockingClassProcessor.class)
                .process("1", tx));

    // Make sure we can schedule more work with a different client key
    txManager.inTransaction(
        tx ->
            outbox
                .with()
                .uniqueRequestId("context-clientkey2")
                .schedule(BlockingClassProcessor.class)
                .process("2", tx));

    // Make sure we can't repeat the same work
    txManager.inTransaction(
        tx ->
            Assertions.assertThrows(
                AlreadyScheduledException.class,
                () ->
                    outbox
                        .with()
                        .uniqueRequestId("context-clientkey1")
                        .schedule(BlockingClassProcessor.class)
                        .process("3", tx)));

    // Run the clock forward to just under the retention threshold
    clockProvider.set(
        Clock.fixed(
            clockProvider.get().instant().plus(Duration.ofDays(2)).minusSeconds(60),
            clockProvider.get().getZone()));
    outbox.flush();

    // Make sure we can schedule more work with a different client key
    txManager.inTransaction(
        tx ->
            outbox
                .with()
                .uniqueRequestId("context-clientkey4")
                .schedule(BlockingClassProcessor.class)
                .process("4", tx));

    // Make sure we still can't repeat the same work
    txManager.inTransaction(
        tx ->
            Assertions.assertThrows(
                AlreadyScheduledException.class,
                () ->
                    outbox
                        .with()
                        .uniqueRequestId("context-clientkey1")
                        .schedule(BlockingClassProcessor.class)
                        .process("5", tx)));

    // Run the clock over the threshold
    clockProvider.set(
        Clock.fixed(clockProvider.get().instant().plusSeconds(120), clockProvider.get().getZone()));
    outbox.flush();

    // We should now be able to add the work
    txManager.inTransaction(
        tx ->
            outbox
                .with()
                .uniqueRequestId("context-clientkey1")
                .schedule(BlockingClassProcessor.class)
                .process("6", tx));

    assertThat(ids, containsInAnyOrder("1", "2", "4", "6"));
  }

  /** Attempts to fire a concrete class via reflection. */
  @Test
  final void testBlockingReflectionInstantiatorConcreteClass() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);

    TransactionOutbox outbox = builder().listener(new LatchListener(latch)).build();

    cleanDataStore();

    BlockingClassProcessor.PROCESSED.clear();
    String myId = UUID.randomUUID().toString();

    txManager.inTransaction(tx -> outbox.schedule(BlockingClassProcessor.class).process(myId, tx));

    assertTrue(latch.await(2, TimeUnit.SECONDS));
    assertEquals(List.of(myId), BlockingClassProcessor.PROCESSED);
  }

  /**
   * Runs a piece of work which will fail several times before working successfully. Ensures that
   * the work runs eventually.
   */
  @Test
  final void testBlockingRetryBehaviourDirectTx() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    TransactionOutbox outbox =
        builder()
            .instantiator(new FailingInstantiator(2))
            .submitter(Submitter.withExecutor(unreliablePool))
            .attemptFrequency(Duration.ofMillis(500))
            .listener(new LatchListener(latch))
            .build();

    cleanDataStore();

    withBlockingRunningFlusher(
        outbox,
        () -> {
          txManager.inTransaction(
              tx -> outbox.schedule(BlockingInterfaceProcessor.class).process(3, "Whee", tx));
          assertFired(latch);
        });
  }

  /**
   * Runs a piece of work which will fail several times before working successfully. Ensures that
   * the work runs eventually.
   */
  @Test
  final void testBlockingRetryBehaviourThreadLocal() throws Exception {
    Assumptions.assumeTrue(supportsThreadLocalContext());

    CountDownLatch latch = new CountDownLatch(1);
    TransactionOutbox outbox =
        builder()
            .instantiator(new FailingInstantiator(2))
            .submitter(Submitter.withExecutor(unreliablePool))
            .attemptFrequency(Duration.ofMillis(500))
            .listener(new LatchListener(latch))
            .build();

    cleanDataStore();

    withBlockingRunningFlusher(
        outbox,
        () -> {
          txManager.inTransaction(
              () -> outbox.schedule(BlockingInterfaceProcessor.class).process(3, "Whee"));
          assertFired(latch);
        });
  }

  /**
   * Runs a piece of work which will fail enough times to be blocked but will then pass when
   * unblocked.
   */
  @Test
  final void testBlockingBlackAndWhitelistThreadLocal() throws Exception {
    Assumptions.assumeTrue(supportsThreadLocalContext());

    CountDownLatch successLatch = new CountDownLatch(1);
    CountDownLatch blockLatch = new CountDownLatch(1);
    LatchListener latchListener = new LatchListener(successLatch, blockLatch);
    TransactionOutbox outbox =
        builder()
            .instantiator(new FailingInstantiator(2))
            .attemptFrequency(Duration.ofMillis(500))
            .listener(latchListener)
            .blockAfterAttempts(2)
            .build();

    cleanDataStore();

    withBlockingRunningFlusher(
        outbox,
        () -> {
          txManager.inTransaction(
              () -> outbox.schedule(BlockingInterfaceProcessor.class).process(3, "Whee"));
          assertFired(blockLatch);
          boolean allowed =
              txManager.inTransactionReturns(
                  tx -> outbox.unblock(latchListener.getBlocked().getId()));
          assertTrue(allowed);
          assertFired(successLatch);
        });
  }

  /**
   * Runs a piece of work which will fail enough times to be blocked but will then pass when
   * unblocked.
   */
  @Test
  final void testBlockingBlackAndWhitelistDirectTx() throws Exception {
    CountDownLatch successLatch = new CountDownLatch(1);
    CountDownLatch blockLatch = new CountDownLatch(1);
    LatchListener latchListener = new LatchListener(successLatch, blockLatch);
    TransactionOutbox outbox =
        builder()
            .instantiator(new FailingInstantiator(2))
            .attemptFrequency(Duration.ofMillis(500))
            .listener(latchListener)
            .blockAfterAttempts(2)
            .build();

    cleanDataStore();

    withBlockingRunningFlusher(
        outbox,
        () -> {
          txManager.inTransaction(
              tx -> outbox.schedule(BlockingInterfaceProcessor.class).process(3, "Whee", tx));
          assertFired(blockLatch);
          boolean unblocked =
              txManager.inTransactionReturns(
                  tx -> outbox.unblock(latchListener.getBlocked().getId(), tx));
          assertTrue(unblocked);
          assertFired(successLatch);
        });
  }

  /** Hammers high-volume, frequently failing tasks to ensure that they all get run. */
  @Test
  final void testBlockingHighVolumeUnreliableThreadLocal() throws Exception {
    Assumptions.assumeTrue(supportsThreadLocalContext());

    int count = 10;

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

    withBlockingRunningFlusher(
        outbox,
        () -> {
          IntStream.range(0, count)
              .parallel()
              .forEach(
                  i ->
                      txManager.inTransaction(
                          () -> {
                            for (int j = 0; j < 10; j++) {
                              outbox
                                  .schedule(BlockingInterfaceProcessor.class)
                                  .process(i * 10 + j, "Whee");
                            }
                          }));
          assertFired(latch, 20);
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
  final void testBlockingHighVolumeUnreliableDirectTx() throws Exception {

    int count = 10;

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

    withBlockingRunningFlusher(
        outbox,
        () -> {
          IntStream.range(0, count)
              .parallel()
              .forEach(
                  i ->
                      txManager.inTransaction(
                          tx -> {
                            for (int j = 0; j < 10; j++) {
                              outbox
                                  .schedule(BlockingInterfaceProcessor.class)
                                  .process(i * 10 + j, "Whee", tx);
                            }
                          }));
          assertFired(latch, 30);
        });

    assertThat(
        "Should never get duplicates running to full completion", duplicates.keySet(), empty());
    assertThat(
        "Only got: " + results.keySet(),
        results.keySet(),
        containsInAnyOrder(IntStream.range(0, count * 10).boxed().toArray()));
  }

  protected HikariDataSource pooledDataSource() {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(connectionDetails().url());
    config.setUsername(connectionDetails().user());
    config.setPassword(connectionDetails().password());
    config.addDataSourceProperty("cachePrepStmts", "true");
    return autoClose(new HikariDataSource(config));
  }

  private void withBlockingRunningFlusher(TransactionOutbox outbox, ThrowingRunnable runnable)
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
