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
import com.gruelbox.transactionoutbox.Instantiator;
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
import com.gruelbox.transactionoutbox.sql.Dialect;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.org.apache.commons.lang.math.RandomUtils;

@Slf4j
abstract class AbstractAcceptanceTestV1 {

  static {
    Async.init();
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractAcceptanceTestV1.class);
  private final ExecutorService unreliablePool =
      new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(16));

  protected abstract ConnectionDetails connectionDetails();

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
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .instantiator(
                Instantiator.using(
                    clazz ->
                        (InterfaceProcessor)
                            (foo, bar) -> LOGGER.info("Processing ({}, {})", foo, bar)))
            .submitter(Submitter.withExecutor(unreliablePool))
            .listener(
                new LatchListener(latch)
                    .andThen(
                        new TransactionOutboxListener() {
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
              .build();

      clearOutbox();
      ClassProcessor.PROCESSED.clear();
      String myId = UUID.randomUUID().toString();

      try {
        outbox.schedule(ClassProcessor.class).process(myId);
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
      assertEquals(List.of(myId), ClassProcessor.PROCESSED);
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
  final void blacklistAndWhitelist() throws Exception {
    TransactionManager transactionManager = simpleTxnManager();
    CountDownLatch successLatch = new CountDownLatch(1);
    CountDownLatch blacklistLatch = new CountDownLatch(1);
    LatchListener latchListener = new LatchListener(successLatch, blacklistLatch);
    AtomicInteger attempts = new AtomicInteger();
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(Persistor.forDialect(connectionDetails().dialect()))
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

  private static class FailingInstantiator implements Instantiator {

    private final AtomicInteger attempts;

    FailingInstantiator(AtomicInteger attempts) {
      this.attempts = attempts;
    }

    @Override
    public String getName(Class<?> clazz) {
      return "BEEF";
    }

    @Override
    public Object getInstance(String name) {
      if (!"BEEF".equals(name)) {
        throw new UnsupportedOperationException();
      }
      return (InterfaceProcessor)
          (foo, bar) -> {
            LOGGER.info("Processing ({}, {})", foo, bar);
            if (attempts.incrementAndGet() < 3) {
              throw new RuntimeException("Temporary failure");
            }
            LOGGER.info("Processed ({}, {})", foo, bar);
          };
    }
  }

  private static class RandomFailingInstantiator implements Instantiator {

    @Override
    public String getName(Class<?> clazz) {
      return clazz.getName();
    }

    @Override
    public Object getInstance(String name) {
      if (InterfaceProcessor.class.getName().equals(name)) {
        return (InterfaceProcessor)
            (foo, bar) -> {
              LOGGER.info("Processing ({}, {})", foo, bar);
              if (RandomUtils.nextInt(10) == 5) {
                throw new RuntimeException("Temporary failure of InterfaceProcessor");
              }
              LOGGER.info("Processed ({}, {})", foo, bar);
            };
      } else {
        throw new UnsupportedOperationException();
      }
    }
  }

  @Value
  @Accessors(fluent = true)
  @Builder
  static class ConnectionDetails {
    String driverClassName;
    String url;
    String user;
    String password;
    Dialect dialect;
  }
}
