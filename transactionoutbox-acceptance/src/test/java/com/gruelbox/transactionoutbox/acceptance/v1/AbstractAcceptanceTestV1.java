package com.gruelbox.transactionoutbox.acceptance.v1;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.gruelbox.transactionoutbox.AlreadyScheduledException;
import com.gruelbox.transactionoutbox.NoTransactionActiveException;
import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.Submitter;
import com.gruelbox.transactionoutbox.ThreadLocalContextTransactionManager;
import com.gruelbox.transactionoutbox.ThrowingRunnable;
import com.gruelbox.transactionoutbox.ThrowingTransactionalSupplier;
import com.gruelbox.transactionoutbox.Transaction;
import com.gruelbox.transactionoutbox.TransactionManager;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.TransactionOutboxListener;
import com.gruelbox.transactionoutbox.acceptance.BlockingClassProcessor;
import com.gruelbox.transactionoutbox.acceptance.BlockingInterfaceProcessor;
import com.gruelbox.transactionoutbox.acceptance.FailingInstantiator;
import com.gruelbox.transactionoutbox.acceptance.JdbcConnectionDetails;
import com.gruelbox.transactionoutbox.acceptance.LatchListener;
import com.gruelbox.transactionoutbox.acceptance.LoggingInstantiator;
import com.gruelbox.transactionoutbox.acceptance.RandomFailingInstantiator;
import com.gruelbox.transactionoutbox.acceptance.TestUtils;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

@SuppressWarnings("deprecation")
@Slf4j
abstract class AbstractAcceptanceTestV1 {

  private final ExecutorService unreliablePool =
      new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(16));

  protected abstract JdbcConnectionDetails connectionDetails();

  /**
   * Uses a simple direct transaction manager and connection manager and attempts to fire an
   * interface using a custom instantiator.
   */
  @Test
  final void simpleConnectionProviderCustomInstantiatorInterfaceClass()
      throws InterruptedException {

    TransactionManager transactionManager = simpleTxnManager();

    CountDownLatch latch = new CountDownLatch(1);
    CountDownLatch chainedLatch = new CountDownLatch(1);
    AtomicBoolean gotScheduled = new AtomicBoolean();
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .instantiator(new LoggingInstantiator())
            .submitter(Submitter.withExecutor(unreliablePool))
            .logLevelProcessStartAndFinish(Level.DEBUG)
            .logLevelTemporaryFailure(Level.DEBUG)
            .listener(
                new LatchListener(latch)
                    .andThen(
                        new TransactionOutboxListener() {

                          @Override
                          public void scheduled(TransactionOutboxEntry entry) {
                            log.info("Got scheduled event");
                            gotScheduled.set(true);
                          }

                          @Override
                          public void success(TransactionOutboxEntry entry) {
                            chainedLatch.countDown();
                          }
                        }))
            .persistor(Persistor.forDialect(connectionDetails().dialect()))
            .build();

    clearOutbox();

    transactionManager.inTransaction(
        () -> {
          outbox.schedule(BlockingInterfaceProcessor.class).process(3, "Whee");
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
    assertTrue(gotScheduled.get());
  }

  @Test
  void duplicateRequests() {

    TransactionManager transactionManager = simpleTxnManager();

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
            .persistor(Persistor.forDialect(connectionDetails().dialect()))
            .retentionThreshold(Duration.ofDays(2))
            .clockProvider(clockProvider::get)
            .logLevelProcessStartAndFinish(Level.DEBUG)
            .logLevelTemporaryFailure(Level.DEBUG)
            .build();

    clearOutbox();

    // Schedule some work
    transactionManager.inTransaction(
        () ->
            outbox
                .with()
                .uniqueRequestId("context-clientkey1")
                .schedule(BlockingClassProcessor.class)
                .process("1"));

    // Make sure we can schedule more work with a different client key
    transactionManager.inTransaction(
        () ->
            outbox
                .with()
                .uniqueRequestId("context-clientkey2")
                .schedule(BlockingClassProcessor.class)
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
                        .schedule(BlockingClassProcessor.class)
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
                .schedule(BlockingClassProcessor.class)
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
                        .schedule(BlockingClassProcessor.class)
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
                .schedule(BlockingClassProcessor.class)
                .process("6"));

    assertThat(ids, containsInAnyOrder("1", "2", "4", "6"));
  }

  /**
   * Uses a simple data source transaction manager and attempts to fire a concrete class via
   * reflection.
   */
  @Test
  final void dataSourceConnectionProviderReflectionInstantiatorConcreteClass()
      throws InterruptedException {
    try (HikariDataSource ds = pooledDataSource()) {

      CountDownLatch latch = new CountDownLatch(1);

      TransactionManager transactionManager = TransactionManager.fromDataSource(ds);
      TransactionOutbox outbox =
          TransactionOutbox.builder()
              .transactionManager(transactionManager)
              .persistor(Persistor.forDialect(connectionDetails().dialect()))
              .listener(new LatchListener(latch))
              .logLevelProcessStartAndFinish(Level.DEBUG)
              .logLevelTemporaryFailure(Level.DEBUG)
              .build();

      clearOutbox();
      BlockingClassProcessor.PROCESSED.clear();
      String myId = UUID.randomUUID().toString();

      transactionManager.inTransaction(
          () -> outbox.schedule(BlockingClassProcessor.class).process(myId));

      assertTrue(latch.await(2, TimeUnit.SECONDS));
      assertEquals(List.of(myId), BlockingClassProcessor.PROCESSED);
    }
  }

  /**
   * Implements a custom transaction manager. Any required changes to this test are a sign that we
   * need to bump the major revision.
   */
  @Test
  final void customTransactionManager()
      throws ClassNotFoundException, SQLException, InterruptedException {

    Class.forName(connectionDetails().driverClassName());
    try (Connection connection =
        DriverManager.getConnection(
            connectionDetails().url(),
            connectionDetails().user(),
            connectionDetails().password())) {

      connection.setAutoCommit(false);
      connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

      ArrayList<Runnable> postCommitHooks = new ArrayList<>();
      ArrayList<PreparedStatement> preparedStatements = new ArrayList<>();
      CountDownLatch latch = new CountDownLatch(1);

      Transaction transaction =
          new Transaction() {
            @Override
            public Connection connection() {
              return connection;
            }

            @Override
            @SneakyThrows
            public PreparedStatement prepareBatchStatement(String sql) {
              var stmt = connection.prepareStatement(sql);
              preparedStatements.add(stmt);
              return stmt;
            }

            @Override
            public void addPostCommitHook(Runnable runnable) {
              postCommitHooks.add(runnable);
            }
          };

      TransactionManager transactionManager =
          new ThreadLocalContextTransactionManager() {
            @Override
            public <T, E extends Exception> T inTransactionReturnsThrows(
                ThrowingTransactionalSupplier<T, E> work) throws E {
              return work.doWork(transaction);
            }

            @Override
            public <T, E extends Exception> T requireTransactionReturns(
                ThrowingTransactionalSupplier<T, E> work) throws E, NoTransactionActiveException {
              return work.doWork(transaction);
            }
          };

      TransactionOutbox outbox =
          TransactionOutbox.builder()
              .transactionManager(transactionManager)
              .listener(new LatchListener(latch))
              .persistor(Persistor.forDialect(connectionDetails().dialect()))
              .logLevelProcessStartAndFinish(Level.DEBUG)
              .logLevelTemporaryFailure(Level.DEBUG)
              .build();

      clearOutbox();
      BlockingClassProcessor.PROCESSED.clear();
      String myId = UUID.randomUUID().toString();

      try {
        outbox.schedule(BlockingClassProcessor.class).process(myId);
        preparedStatements.forEach(
            it -> {
              try {
                it.executeBatch();
                it.close();
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            });
        connection.commit();
      } catch (Exception e) {
        connection.rollback();
        throw e;
      }
      postCommitHooks.forEach(Runnable::run);

      assertTrue(latch.await(2, TimeUnit.SECONDS));
      assertEquals(List.of(myId), BlockingClassProcessor.PROCESSED);
    }
  }

  /**
   * Runs a piece of work which will fail several times before working successfully. Ensures that
   * the work runs eventually.
   */
  @Test
  final void retryBehaviour() throws Exception {
    TransactionManager transactionManager = simpleTxnManager();
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger attempts = new AtomicInteger();
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(Persistor.forDialect(connectionDetails().dialect()))
            .instantiator(new FailingInstantiator(2))
            .submitter(Submitter.withExecutor(unreliablePool))
            .attemptFrequency(Duration.ofMillis(500))
            .listener(new LatchListener(latch))
            .logLevelProcessStartAndFinish(Level.DEBUG)
            .logLevelTemporaryFailure(Level.DEBUG)
            .build();

    clearOutbox();

    withRunningFlusher(
        outbox,
        () -> {
          transactionManager.inTransaction(
              () -> outbox.schedule(BlockingInterfaceProcessor.class).process(3, "Whee"));
          assertTrue(latch.await(15, TimeUnit.SECONDS));
        });
  }

  @Test
  final void lastAttemptTime_updatesEveryTime() throws Exception {
    TransactionManager transactionManager = simpleTxnManager();
    CountDownLatch successLatch = new CountDownLatch(1);
    CountDownLatch blockLatch = new CountDownLatch(1);
    var orderedEntryListener = new OrderedEntryListener(successLatch, blockLatch);
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(Persistor.forDialect(connectionDetails().dialect()))
            .instantiator(new FailingInstantiator(2))
            .submitter(Submitter.withExecutor(unreliablePool))
            .attemptFrequency(Duration.ofMillis(500))
            .listener(orderedEntryListener)
            .blockAfterAttempts(2)
            .logLevelProcessStartAndFinish(Level.DEBUG)
            .logLevelTemporaryFailure(Level.DEBUG)
            .build();

    clearOutbox();

    withRunningFlusher(
        outbox,
        () -> {
          transactionManager.inTransaction(
              () -> outbox.schedule(BlockingInterfaceProcessor.class).process(3, "Whee"));
          assertTrue(blockLatch.await(10, TimeUnit.SECONDS));
          assertTrue(
              transactionManager.inTransactionReturns(
                  tx -> outbox.unblock(orderedEntryListener.getBlocked().getId())));
          assertTrue(successLatch.await(10, TimeUnit.SECONDS));
          var orderedEntryEvents = orderedEntryListener.getOrderedEntries();
          log.info("The entry life cycle is: {}", orderedEntryEvents);

          // then we are only dealing in terms of a single outbox entry.
          assertEquals(
              1, orderedEntryEvents.stream().map(TransactionOutboxEntry::getId).distinct().count());
          // the first, scheduled entry has no lastAttemptTime set
          assertNull(orderedEntryEvents.get(0).getLastAttemptTime());
          // all subsequent entries (2 x failures (second of which 'blocks'), 1x success updates
          // against db) have a distinct lastAttemptTime set on them.
          assertEquals(
              3,
              orderedEntryEvents.stream()
                  .skip(1)
                  .map(TransactionOutboxEntry::getLastAttemptTime)
                  .distinct()
                  .count());
        });
  }

  /**
   * Runs a piece of work which will fail enough times to be blocked but will then pass when
   * unblocked.
   */
  @Test
  final void blockAndUnblock() throws Exception {
    TransactionManager transactionManager = simpleTxnManager();
    CountDownLatch successLatch = new CountDownLatch(1);
    CountDownLatch blockLatch = new CountDownLatch(1);
    LatchListener latchListener = new LatchListener(successLatch, blockLatch);
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(Persistor.forDialect(connectionDetails().dialect()))
            .instantiator(new FailingInstantiator(2))
            .attemptFrequency(Duration.ofMillis(500))
            .listener(latchListener)
            .blockAfterAttempts(2)
            .logLevelProcessStartAndFinish(Level.DEBUG)
            .logLevelTemporaryFailure(Level.DEBUG)
            .build();

    clearOutbox();

    withRunningFlusher(
        outbox,
        () -> {
          transactionManager.inTransaction(
              () -> outbox.schedule(BlockingInterfaceProcessor.class).process(3, "Whee"));
          assertTrue(blockLatch.await(3, TimeUnit.SECONDS));
          assertTrue(
              transactionManager.inTransactionReturns(
                  tx -> outbox.unblock(latchListener.getBlocked().getId())));
          assertTrue(successLatch.await(3, TimeUnit.SECONDS));
        });
  }

  /** Hammers high-volume, frequently failing tasks to ensure that they all get run. */
  @Test
  final void highVolumeUnreliable() throws Exception {
    int count = 10;

    TransactionManager transactionManager = simpleTxnManager();
    CountDownLatch latch = new CountDownLatch(count * 10);
    ConcurrentHashMap<Integer, Integer> results = new ConcurrentHashMap<>();
    ConcurrentHashMap<Integer, Integer> duplicates = new ConcurrentHashMap<>();

    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(Persistor.forDialect(connectionDetails().dialect()))
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
            .logLevelProcessStartAndFinish(Level.DEBUG)
            .logLevelTemporaryFailure(Level.DEBUG)
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
                              outbox
                                  .schedule(BlockingInterfaceProcessor.class)
                                  .process(i * 10 + j, "Whee");
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

  private TransactionManager simpleTxnManager() {
    return TransactionManager.fromConnectionDetails(
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
