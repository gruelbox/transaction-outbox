package com.gruelbox.transactionoutbox.testing;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.*;

import com.gruelbox.transactionoutbox.*;
import com.gruelbox.transactionoutbox.spi.Utils;
import com.zaxxer.hikari.HikariDataSource;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

@Slf4j
public abstract class AbstractAcceptanceTest extends BaseTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractAcceptanceTest.class);

  private ExecutorService unreliablePool;
  private ExecutorService singleThreadPool;

  private static final Random random = new Random();

  @BeforeEach
  void beforeEachBase() {
    unreliablePool =
        new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(16));
    singleThreadPool = Executors.newSingleThreadExecutor();
  }

  @AfterEach
  void afterEachBase() throws InterruptedException {
    unreliablePool.shutdown();
    singleThreadPool.shutdown();
    assertTrue(unreliablePool.awaitTermination(30, SECONDS));
    assertTrue(singleThreadPool.awaitTermination(30, SECONDS));
  }

  @Test
  final void testMDCPassedToTask() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    var transactionManager = txManager();
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .instantiator(
                Instantiator.using(
                    clazz ->
                        (InterfaceProcessor)
                            (foo, bar) -> {
                              log.info("Processing ({}, {})", foo, bar);
                              assertEquals("Foo", MDC.get("SESSION-KEY"));
                            }))
            .listener(new LatchListener(latch))
            .persistor(StubPersistor.builder().build())
            .build();

    MDC.put("SESSION-KEY", "Foo");
    try {
      transactionManager.inTransaction(
          () -> outbox.schedule(InterfaceProcessor.class).process(3, "Whee"));
    } finally {
      MDC.clear();
    }

    assertTrue(latch.await(2, TimeUnit.SECONDS));
  }

  @Test
  final void sequencing() throws Exception {
    int countPerTopic = 20;
    int topicCount = 5;

    AtomicInteger insertIndex = new AtomicInteger();
    CountDownLatch latch = new CountDownLatch(countPerTopic * topicCount);
    ThreadLocalContextTransactionManager transactionManager =
        (ThreadLocalContextTransactionManager) txManager();

    transactionManager.inTransaction(
        tx -> {
          //noinspection resource
          try (var stmt = tx.connection().createStatement()) {
            stmt.execute("DROP TABLE TEST_TABLE");
          } catch (SQLException e) {
            // ignore
          }
        });

    transactionManager.inTransaction(
        tx -> {
          //noinspection resource
          try (var stmt = tx.connection().createStatement()) {
            stmt.execute(createTestTable());
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });

    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .submitter(Submitter.withExecutor(unreliablePool))
            .attemptFrequency(Duration.ofMillis(500))
            .instantiator(
                new RandomFailingInstantiator(
                    (foo, bar) -> {
                      transactionManager.requireTransaction(
                          tx -> {
                            //noinspection resource
                            try (var stmt =
                                tx.connection()
                                    .prepareStatement(
                                        "INSERT INTO TEST_TABLE (topic, ix, foo) VALUES(?, ?, ?)")) {
                              stmt.setString(1, bar);
                              stmt.setInt(2, insertIndex.incrementAndGet());
                              stmt.setInt(3, foo);
                              stmt.executeUpdate();
                            } catch (SQLException e) {
                              throw new RuntimeException(e);
                            }
                          });
                    }))
            .persistor(persistor())
            .listener(new LatchListener(latch))
            .initializeImmediately(false)
            .flushBatchSize(4)
            .build();

    outbox.initialize();
    clearOutbox();

    withRunningFlusher(
        outbox,
        () -> {
          transactionManager.inTransaction(
              () -> {
                for (int i = 1; i <= countPerTopic; i++) {
                  for (int j = 1; j <= topicCount; j++) {
                    outbox
                        .with()
                        .ordered("topic" + j)
                        .schedule(InterfaceProcessor.class)
                        .process(i, "topic" + j);
                  }
                }
              });
          assertTrue(latch.await(30, SECONDS));
        });

    var output = new HashMap<String, ArrayList<Integer>>();
    transactionManager.inTransaction(
        tx -> {
          //noinspection resource
          try (var stmt = tx.connection().createStatement();
              var rs = stmt.executeQuery("SELECT topic, foo FROM TEST_TABLE ORDER BY ix")) {
            while (rs.next()) {
              ArrayList<Integer> values =
                  output.computeIfAbsent(rs.getString(1), k -> new ArrayList<>());
              values.add(rs.getInt(2));
            }
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });

    var indexes = IntStream.range(1, countPerTopic + 1).boxed().collect(toList());
    var expected =
        IntStream.range(1, topicCount + 1)
            .mapToObj(i -> "topic" + i)
            .collect(toMap(it -> it, it -> indexes));
    assertEquals(expected, output);
  }

    @Test
    final void batchSequencing() throws Exception {
        int countPerTopic = 20;
        int topicCount = 5;

        AtomicInteger insertIndex = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(countPerTopic * topicCount);
        ThreadLocalContextTransactionManager transactionManager =
                (ThreadLocalContextTransactionManager) txManager();

        transactionManager.inTransaction(
                tx -> {
                    //noinspection resource
                    try (var stmt = tx.connection().createStatement()) {
                        stmt.execute("DROP TABLE TEST_TABLE");
                    } catch (SQLException e) {
                        // ignore
                    }
                });

        transactionManager.inTransaction(
                tx -> {
                    //noinspection resource
                    try (var stmt = tx.connection().createStatement()) {
                        stmt.execute(createTestTable());
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });

        TransactionOutbox outbox =
                TransactionOutbox.builder()
                        .transactionManager(transactionManager)
                        .submitter(Submitter.withExecutor(unreliablePool))
                        .attemptFrequency(Duration.ofMillis(500))
                        .instantiator(
                                new RandomFailingInstantiator(
                                        (foo, bar) -> {
                                            transactionManager.requireTransaction(
                                                    tx -> {
                                                        //noinspection resource
                                                        try (var stmt =
                                                                     tx.connection()
                                                                             .prepareStatement(
                                                                                     "INSERT INTO TEST_TABLE (topic, ix, foo) VALUES(?, ?, ?)")) {
                                                            stmt.setString(1, bar);
                                                            stmt.setInt(2, insertIndex.incrementAndGet());
                                                            stmt.setInt(3, foo);
                                                            stmt.executeUpdate();
                                                        } catch (SQLException e) {
                                                            throw new RuntimeException(e);
                                                        }
                                                    });
                                        }))
                        .persistor(persistor())
                        .listener(new LatchListener(latch))
                        .initializeImmediately(false)
                        .flushBatchSize(4)
                        .useOrderedBatchProcessing(true)
                        .build();

        outbox.initialize();
        clearOutbox();

        withRunningFlusher(
                outbox,
                () -> {
                    transactionManager.inTransaction(
                            () -> {
                                for (int i = 1; i <= countPerTopic; i++) {
                                    for (int j = 1; j <= topicCount; j++) {
                                        outbox
                                                .with()
                                                .ordered("topic" + j)
                                                .schedule(InterfaceProcessor.class)
                                                .process(i, "topic" + j);
                                    }
                                }
                            });
                    assertTrue(latch.await(30, SECONDS));
                });

        var output = new HashMap<String, ArrayList<Integer>>();
        transactionManager.inTransaction(
                tx -> {
                    //noinspection resource
                    try (var stmt = tx.connection().createStatement();
                         var rs = stmt.executeQuery("SELECT topic, foo FROM TEST_TABLE ORDER BY ix")) {
                        while (rs.next()) {
                            ArrayList<Integer> values =
                                    output.computeIfAbsent(rs.getString(1), k -> new ArrayList<>());
                            values.add(rs.getInt(2));
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });

        var indexes = IntStream.range(1, countPerTopic + 1).boxed().collect(toList());
        var expected =
                IntStream.range(1, topicCount + 1)
                        .mapToObj(i -> "topic" + i)
                        .collect(toMap(it -> it, it -> indexes));
        assertEquals(expected, output);
    }

  /**
   * Uses a simple direct transaction manager and connection manager and attempts to fire an
   * interface using a custom instantiator.
   */
  @Test
  final void simpleConnectionProviderCustomInstantiatorInterfaceClass()
      throws InterruptedException {

    TransactionManager transactionManager = txManager();

    CountDownLatch latch = new CountDownLatch(1);
    CountDownLatch chainedLatch = new CountDownLatch(1);
    AtomicBoolean gotScheduled = new AtomicBoolean();
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
                          public void scheduled(TransactionOutboxEntry entry) {
                            log.info("Got scheduled event");
                            gotScheduled.set(true);
                          }

                          @Override
                          public void success(TransactionOutboxEntry entry) {
                            chainedLatch.countDown();
                          }
                        }))
            .persistor(persistor())
            .initializeImmediately(false)
            .build();

    outbox.initialize();
    clearOutbox();

    transactionManager.inTransaction(
        () -> {
          outbox.schedule(InterfaceProcessor.class).process(3, "Whee");
          try {
            // Should not be fired until after commit
            assertFalse(latch.await(2, SECONDS));
          } catch (InterruptedException e) {
            fail("Interrupted");
          }
        });

    // Should be fired after commit
    assertTrue(chainedLatch.await(2, SECONDS));
    assertTrue(latch.await(1, SECONDS));
    assertTrue(gotScheduled.get());
  }

  @Test
  final void noAutomaticInitialization() {

    TransactionManager transactionManager = txManager();
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .instantiator(
                Instantiator.using(
                    clazz ->
                        (InterfaceProcessor)
                            (foo, bar) -> LOGGER.info("Processing ({}, {})", foo, bar)))
            .submitter(Submitter.withDefaultExecutor())
            .persistor(Persistor.forDialect(connectionDetails().dialect()))
            .initializeImmediately(false)
            .build();

    Persistor.forDialect(connectionDetails().dialect()).migrate(txManager());
    clearOutbox();

    Assertions.assertThrows(
        IllegalStateException.class,
        () ->
            transactionManager.inTransaction(
                () -> outbox.schedule(InterfaceProcessor.class).process(3, "Whee")));
  }

  @Test
  void duplicateRequests() {

    TransactionManager transactionManager = txManager();

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
    try (HikariDataSource ds = dataSource) {

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

      assertTrue(latch.await(2, SECONDS));
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

      assertTrue(latch.await(2, SECONDS));
      assertEquals(List.of(myId), ClassProcessor.PROCESSED);
    }
  }

  /**
   * Runs a piece of work which will fail several times before working successfully. Ensures that
   * the work runs eventually.
   */
  @Test
  final void retryBehaviour() throws Exception {
    TransactionManager transactionManager = txManager();
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger attempts = new AtomicInteger();
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(Persistor.forDialect(connectionDetails().dialect()))
            .instantiator(new FailingInstantiator(attempts))
            .submitter(Submitter.withExecutor(singleThreadPool))
            .attemptFrequency(Duration.ofMillis(500))
            .listener(new LatchListener(latch))
            .build();

    clearOutbox();

    withRunningFlusher(
        outbox,
        () -> {
          transactionManager.inTransaction(
              () -> outbox.schedule(InterfaceProcessor.class).process(3, "Whee"));
          assertTrue(latch.await(15, SECONDS));
        },
        singleThreadPool);
  }

  @Test
  final void flushOnlyASpecifiedTopic() throws Exception {
    TransactionManager transactionManager = txManager();
    CountDownLatch successLatch = new CountDownLatch(1);
    var processedEntryListener = new ProcessedEntryListener(successLatch);
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(Persistor.forDialect(connectionDetails().dialect()))
            .instantiator(
                Instantiator.using(
                    clazz ->
                        (InterfaceProcessor)
                            (foo, bar) ->
                                LOGGER.info(
                                    "Entered the method to process successfully. Processing ({}, {})",
                                    foo,
                                    bar)))
            .submitter(Submitter.withExecutor(singleThreadPool))
            .attemptFrequency(Duration.ofMillis(500))
            .listener(processedEntryListener)
            .build();

    clearOutbox();

    var selectedTopic = "SELECTED_TOPIC";
    transactionManager.inTransaction(
        () -> {
          outbox
              .with()
              .ordered(selectedTopic)
              .schedule(InterfaceProcessor.class)
              .process(1, "Whoo");
          outbox
              .with()
              .ordered("IGNORED_TOPIC")
              .schedule(InterfaceProcessor.class)
              .process(2, "Wheeeee");
        });
    assertFalse(
        successLatch.await(5, SECONDS),
        "At this point, nothing should have been picked up for processing");

    outbox.flushTopics(singleThreadPool, selectedTopic);

    assertTrue(successLatch.await(5, SECONDS), "Should have successfully processed something");

    var successes = processedEntryListener.getSuccessfulEntries();
    var failures = processedEntryListener.getFailingEntries();

    // then we only expect the selected topic we're flushing to have had eventually succeeded
    // as the other work would not have been picked up for a retry
    assertEquals(1, successes.stream().map(TransactionOutboxEntry::getTopic).distinct().count());
    assertEquals(selectedTopic, successes.get(0).getTopic());

    // no failures expected
    assertEquals(0, failures.size());
  }

  @Test
  final void onSchedulingFailure_BubbleExceptionsUp() throws Exception {
    Assumptions.assumeTrue(
        Dialect.MY_SQL_8.equals(connectionDetails().dialect())
            || Dialect.MY_SQL_5.equals(connectionDetails().dialect()));

    TransactionManager transactionManager = txManager();
    CountDownLatch latch = new CountDownLatch(1);
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .instantiator(
                Instantiator.using(
                    clazz ->
                        (InterfaceProcessor)
                            (foo, bar) ->
                                LOGGER.info(
                                    "Entered the method to process successfully. Processing ({}, {})",
                                    foo,
                                    bar)))
            .persistor(Persistor.forDialect(connectionDetails().dialect()))
            .submitter(Submitter.withExecutor(unreliablePool))
            .attemptFrequency(Duration.ofMillis(500))
            .listener(new LatchListener(latch))
            .build();

    clearOutbox();

    withRunningFlusher(
        outbox,
        () ->
            assertThrows(
                Exception.class,
                () ->
                    transactionManager.inTransaction(
                        () ->
                            outbox
                                .with()
                                .uniqueRequestId("some_unique_id")
                                .schedule(InterfaceProcessor.class)
                                .process(1, "This invocation is too long".repeat(650000)))));
  }

  @Test
  final void lastAttemptTime_updatesEveryTime() throws Exception {
    TransactionManager transactionManager = txManager();
    CountDownLatch successLatch = new CountDownLatch(1);
    CountDownLatch blockLatch = new CountDownLatch(1);
    AtomicInteger attempts = new AtomicInteger();
    var orderedEntryListener = new OrderedEntryListener(successLatch, blockLatch);
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(Persistor.forDialect(connectionDetails().dialect()))
            .instantiator(new FailingInstantiator(attempts))
            .submitter(Submitter.withExecutor(singleThreadPool))
            .attemptFrequency(Duration.ofMillis(500))
            .listener(orderedEntryListener)
            .blockAfterAttempts(2)
            .build();

    clearOutbox();

    withRunningFlusher(
        outbox,
        () -> {
          transactionManager.inTransaction(
              () -> outbox.schedule(InterfaceProcessor.class).process(3, "Whee"));
          assertTrue(blockLatch.await(20, SECONDS), "Entry was not blocked");
          assertTrue(
              (Boolean)
                  transactionManager.inTransactionReturns(
                      tx -> outbox.unblock(orderedEntryListener.getBlocked().getId())));
          assertTrue(successLatch.await(20, SECONDS), "Timeout waiting for success");
          var events = orderedEntryListener.getEvents();
          log.info("The entry life cycle is: {}", events);

          // then we are only dealing in terms of a single outbox entry.
          assertEquals(1, events.stream().map(TransactionOutboxEntry::getId).distinct().count());
          // the first, scheduled entry has no lastAttemptTime set
          assertNull(events.get(0).getLastAttemptTime());
          // all subsequent entries (2 x failures (second of which 'blocks'), 1x success updates
          // against db) have a distinct lastAttemptTime set on them.
          assertEquals(
              3,
              events.stream()
                  .skip(1)
                  .map(TransactionOutboxEntry::getLastAttemptTime)
                  .distinct()
                  .count());
        },
        singleThreadPool);
  }

  /**
   * Runs a piece of work which will fail enough times to enter a blocked state but will then pass
   * when re-tried after it is unblocked.
   */
  @Test
  final void blockAndThenUnblockForRetry() throws Exception {
    TransactionManager transactionManager = txManager();
    CountDownLatch successLatch = new CountDownLatch(1);
    CountDownLatch blockLatch = new CountDownLatch(1);
    LatchListener latchListener = new LatchListener(successLatch, blockLatch);
    AtomicInteger attempts = new AtomicInteger();
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(Persistor.forDialect(connectionDetails().dialect()))
            .instantiator(new FailingInstantiator(attempts))
            .submitter(Submitter.withExecutor(singleThreadPool))
            .attemptFrequency(Duration.ofMillis(500))
            .listener(latchListener)
            .blockAfterAttempts(2)
            .build();

    clearOutbox();

    withRunningFlusher(
        outbox,
        () -> {
          transactionManager.inTransaction(
              () -> outbox.schedule(InterfaceProcessor.class).process(3, "Whee"));
          assertTrue(blockLatch.await(5, SECONDS));
          assertTrue(
              (Boolean)
                  transactionManager.inTransactionReturns(
                      tx -> outbox.unblock(latchListener.getBlocked().getId())));
          assertTrue(successLatch.await(5, SECONDS));
        },
        singleThreadPool);
  }

  /** Hammers high-volume, frequently failing tasks to ensure that they all get run. */
  @Test
  final void highVolumeUnreliable() throws Exception {
    int count = 10;

    TransactionManager transactionManager = txManager();
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
          assertTrue(latch.await(30, SECONDS), "Latch not opened in time");
        });

    assertThat(
        "Should never get duplicates running to full completion", duplicates.keySet(), empty());
    assertThat(
        "Only got: " + results.keySet(),
        results.keySet(),
        containsInAnyOrder(IntStream.range(0, count * 10).boxed().toArray()));
  }

  protected String createTestTable() {
    return "CREATE TABLE TEST_TABLE (topic VARCHAR(50), ix INTEGER, foo INTEGER, PRIMARY KEY (topic, ix))";
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

    private final InterfaceProcessor interfaceProcessor;

    RandomFailingInstantiator() {
      this.interfaceProcessor = (foo, bar) -> {};
    }

    RandomFailingInstantiator(InterfaceProcessor interfaceProcessor) {
      this.interfaceProcessor = interfaceProcessor;
    }

    @Override
    public String getName(Class<?> clazz) {
      return clazz.getName();
    }

    @Override
    public Object getInstance(String name) {
      if (InterfaceProcessor.class.getName().equals(name)) {
        return (InterfaceProcessor)
            (foo, bar) -> {
              if (random.nextInt(10) == 5) {
                throw new RuntimeException("Temporary failure of InterfaceProcessor");
              }
              interfaceProcessor.process(foo, bar);
            };
      } else {
        throw new UnsupportedOperationException();
      }
    }
  }

  @Test
  void runWithParentOtelSpan() throws Exception {
    OpenTelemetryExtension otelTesting = OpenTelemetryExtension.create();
    OpenTelemetry otel = otelTesting.getOpenTelemetry();
    otelTesting.beforeAll(null);
    try {
      otelTesting.beforeEach(null);

      var latch = new CountDownLatch(1);
      AtomicReference<SpanContext> remotedSpan = new AtomicReference<>();

      var txManager = txManager();
      var outbox =
          TransactionOutbox.builder()
              .transactionManager(txManager)
              .persistor(Persistor.forDialect(connectionDetails().dialect()))
              .instantiator(
                  Instantiator.using(
                      clazz ->
                          (InterfaceProcessor)
                              (foo, bar) -> {
                                remotedSpan.set(Span.current().getSpanContext());
                              }))
              .attemptFrequency(Duration.ofMillis(500))
              .listener(new OtelListener().andThen(new LatchListener(latch)))
              .blockAfterAttempts(2)
              .build();

      // Start a parent span, which should be propagated to the instantiator above
      Span parentSpan = otel.getTracer("parent-tracer").spanBuilder("parent-span").startSpan();
      String parentTraceId = null;
      try (Scope scope = parentSpan.makeCurrent()) {
        parentTraceId = Span.current().getSpanContext().getTraceId();
        txManager.inTransaction(() -> outbox.schedule(InterfaceProcessor.class).process(1, "1"));
      } finally {
        parentSpan.end();
      }

      // Wait for the job to complete
      assertTrue(latch.await(10, TimeUnit.SECONDS));

      SpanData remotedSpanData = null;
      for (int i = 0; i < 5; i++) {
        remotedSpanData =
            otelTesting.getSpans().stream()
                .filter(it -> it.getSpanId().equals(remotedSpan.get().getSpanId()))
                .findFirst()
                .orElse(null);
        if (remotedSpanData == null) {
          if (i == 4) {
            throw new RuntimeException("No matching span");
          } else {
            Thread.sleep(500);
          }
        }
      }

      // Check they ran with linked traces and the correct class/method/args
      assertTrue(
          remotedSpanData.getLinks().stream()
              .findFirst()
              .orElseThrow(() -> new RuntimeException("No linked trace"))
              .getSpanContext()
              .getTraceId()
              .equals(parentTraceId));
      assertTrue(
          remotedSpanData
              .getName()
              .equals("com.gruelbox.transactionoutbox.testing.InterfaceProcessor.process"));
      assertTrue(remotedSpanData.getAttributes().get(AttributeKey.stringKey("arg0")).equals("1"));
      assertTrue(
          remotedSpanData.getAttributes().get(AttributeKey.stringKey("arg1")).equals("\"1\""));
    } finally {
      otelTesting.afterAll(null);
    }
  }

  @Test
  void runWithoutParentOtelSpan() throws Exception {
    OpenTelemetryExtension otelTesting = OpenTelemetryExtension.create();
    OpenTelemetry otel = otelTesting.getOpenTelemetry();
    otelTesting.beforeAll(null);
    try {
      otelTesting.beforeEach(null);

      var latch = new CountDownLatch(1);
      AtomicReference<SpanContext> remotedSpan = new AtomicReference<>();

      var txManager = txManager();
      var outbox =
          TransactionOutbox.builder()
              .transactionManager(txManager)
              .persistor(Persistor.forDialect(connectionDetails().dialect()))
              .instantiator(
                  Instantiator.using(
                      clazz ->
                          (InterfaceProcessor)
                              (foo, bar) -> remotedSpan.set(Span.current().getSpanContext())))
              .attemptFrequency(Duration.ofMillis(500))
              .listener(new OtelListener().andThen(new LatchListener(latch)))
              .blockAfterAttempts(2)
              .build();

      // Run with no parent span
      txManager.inTransaction(() -> outbox.schedule(InterfaceProcessor.class).process(1, "1"));

      // Wait for the job to complete
      assertTrue(latch.await(10, TimeUnit.SECONDS));

      SpanData remotedSpanData = null;
      for (int i = 0; i < 5; i++) {
        remotedSpanData =
            otelTesting.getSpans().stream()
                .filter(it -> it.getSpanId().equals(remotedSpan.get().getSpanId()))
                .findFirst()
                .orElse(null);
        if (remotedSpanData == null) {
          if (i == 4) {
            throw new RuntimeException("No matching span");
          } else {
            Thread.sleep(500);
          }
        }
      }

      // Check they ran with linked traces and the correct class/method/args
      assertFalse(remotedSpanData.getLinks().stream().findFirst().isPresent());
      assertTrue(
          remotedSpanData
              .getName()
              .equals("com.gruelbox.transactionoutbox.testing.InterfaceProcessor.process"));
      assertTrue(remotedSpanData.getAttributes().get(AttributeKey.stringKey("arg0")).equals("1"));
      assertTrue(
          remotedSpanData.getAttributes().get(AttributeKey.stringKey("arg1")).equals("\"1\""));

    } finally {
      otelTesting.afterAll(null);
    }
  }

  /** Example {@link TransactionOutboxListener} to propagate traces */
  static class OtelListener implements TransactionOutboxListener {

    /** Serialises the current context into {@link Invocation#getSession()}. */
    @Override
    public Map<String, String> extractSession() {
      var result = new HashMap<String, String>();
      SpanContext spanContext = Span.current().getSpanContext();
      if (!spanContext.isValid()) {
        return null;
      }
      result.put("traceId", spanContext.getTraceId());
      result.put("spanId", spanContext.getSpanId());
      log.info("Extracted: {}", result);
      return result;
    }

    /**
     * Deserialises {@link Invocation#getSession()} and sets it as the current context so that any
     * new span started by the method we invoke will treat it as the parent span
     */
    @Override
    public void wrapInvocationAndInit(Invocator invocator) {
      Invocation inv = invocator.getInvocation();
      var spanBuilder =
          GlobalOpenTelemetry.get()
              .getTracer("transaction-outbox")
              .spanBuilder(String.format("%s.%s", inv.getClassName(), inv.getMethodName()))
              .setNoParent();
      for (var i = 0; i < inv.getArgs().length; i++) {
        spanBuilder.setAttribute("arg" + i, Utils.stringify(inv.getArgs()[i]));
      }
      if (inv.getSession() != null) {
        var traceId = inv.getSession().get("traceId");
        var spanId = inv.getSession().get("spanId");
        if (traceId != null && spanId != null) {
          spanBuilder.addLink(
              SpanContext.createFromRemoteParent(
                  traceId, spanId, TraceFlags.getSampled(), TraceState.getDefault()));
        }
      }
      var span = spanBuilder.startSpan();
      try (Scope scope = span.makeCurrent()) {
        invocator.runUnchecked();
      } finally {
        span.end();
      }
    }
  }
}
