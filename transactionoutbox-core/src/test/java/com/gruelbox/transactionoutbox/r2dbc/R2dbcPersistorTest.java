package com.gruelbox.transactionoutbox.r2dbc;

import static com.ea.async.Async.await;
import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.MILLIS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.ea.async.Async;
import com.gruelbox.transactionoutbox.AlreadyScheduledException;
import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.Invocation;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.r2dbc.R2dbcRawTransactionManager.ConnectionFactoryWrapper;
import io.r2dbc.h2.H2ConnectionConfiguration;
import io.r2dbc.h2.H2ConnectionFactory;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
class R2dbcPersistorTest {

  private Instant now = now();

  private final Dialect dialect = Dialect.H2;
  private final R2dbcPersistor persistor = R2dbcPersistor.forDialect(dialect);
  private final ConnectionFactoryWrapper connectionFactory =
      R2dbcRawTransactionManager.wrapConnectionFactory(
          new H2ConnectionFactory(
              H2ConnectionConfiguration.builder()
                  .inMemory("test")
                  .option("DB_CLOSE_DELAY=-1")
                  .build()));
  private final R2dbcRawTransactionManager txManager =
      new R2dbcRawTransactionManager(connectionFactory);

  @BeforeEach
  void beforeAll() throws ExecutionException, InterruptedException {
    Async.init();
    persistor
        .migrate(txManager)
        .thenCompose(__ -> txManager.transactionally(persistor::clear))
        .get();
  }

  @Test
  void testInsertAndSelect() throws ExecutionException, InterruptedException {
    TransactionOutboxEntry entry = createEntry("FOO", now, false);
    List<TransactionOutboxEntry> entries =
        txManager
            .transactionally(tx -> persistor.save(tx, entry))
            .thenCompose(
                __ ->
                    txManager.transactionally(
                        tx -> persistor.selectBatch(tx, 100, now.plusMillis(1))))
            .get();
    assertThat(entries, contains(entry));
  }

  @Test
  void testInsertDuplicate() {
    TransactionOutboxEntry entry1 = createEntry("FOO1", now, false, "context-clientkey1");
    await(txManager.transactionally(tx -> persistor.save(tx, entry1)));
    List<TransactionOutboxEntry> entries =
        await(txManager.transactionally(tx -> persistor.selectBatch(tx, 100, now.plusMillis(1))));
    assertThat(entries, contains(entry1));

    TransactionOutboxEntry entry2 = createEntry("FOO2", now, false, "context-clientkey2");
    await(txManager.transactionally(tx -> persistor.save(tx, entry2)));
    entries =
        await(txManager.transactionally(tx -> persistor.selectBatch(tx, 100, now.plusMillis(1))));
    assertThat(entries, contains(entry1, entry2));

    TransactionOutboxEntry entry3 = createEntry("FOO3", now, false, "context-clientkey1");
    assertThat(
        assertThrows(
                CompletionException.class,
                () -> await(txManager.transactionally(tx -> persistor.save(tx, entry3))))
            .getCause(),
        isA(AlreadyScheduledException.class));

    entries =
        await(txManager.transactionally(tx -> persistor.selectBatch(tx, 100, now.plusMillis(1))));
    assertThat(entries, contains(entry1, entry2));
  }
  //
  //  @Test
  //  void testBatchLimitUnderThreshold() throws Exception {
  //    txManager()
  //        .inTransactionThrows(
  //            tx -> {
  //              persistor().saveBlocking(tx, createEntry("FOO1", now, false));
  //              persistor().saveBlocking(tx, createEntry("FOO2", now, false));
  //              persistor().saveBlocking(tx, createEntry("FOO3", now, false));
  //            });
  //    txManager()
  //        .inTransactionThrows(
  //            tx ->
  //                assertThat(persistor().selectBatchBlocking(tx, 2, now.plusMillis(1)),
  // hasSize(2)));
  //  }
  //
  //  @Test
  //  void testBatchLimitMatchingThreshold() throws Exception {
  //    txManager()
  //        .inTransactionThrows(
  //            tx -> {
  //              persistor().saveBlocking(tx, createEntry("FOO1", now, false));
  //              persistor().saveBlocking(tx, createEntry("FOO2", now, false));
  //              persistor().saveBlocking(tx, createEntry("FOO3", now, false));
  //            });
  //    txManager()
  //        .inTransactionThrows(
  //            tx ->
  //                assertThat(persistor().selectBatchBlocking(tx, 3, now.plusMillis(1)),
  // hasSize(3)));
  //  }
  //
  //  @Test
  //  void testBatchLimitOverThreshold() throws Exception {
  //    txManager()
  //        .inTransactionThrows(
  //            tx -> {
  //              persistor().saveBlocking(tx, createEntry("FOO1", now, false));
  //              persistor().saveBlocking(tx, createEntry("FOO2", now, false));
  //              persistor().saveBlocking(tx, createEntry("FOO3", now, false));
  //            });
  //    txManager()
  //        .inTransactionThrows(
  //            tx ->
  //                assertThat(persistor().selectBatchBlocking(tx, 4, now.plusMillis(1)),
  // hasSize(3)));
  //  }
  //
  //  @Test
  //  void testBatchHorizon() throws Exception {
  //    txManager()
  //        .inTransactionThrows(
  //            tx -> {
  //              persistor().saveBlocking(tx, createEntry("FOO1", now, false));
  //              persistor().saveBlocking(tx, createEntry("FOO2", now, false));
  //              persistor().saveBlocking(tx, createEntry("FOO3", now.plusMillis(2), false));
  //            });
  //    txManager()
  //        .inTransactionThrows(
  //            tx ->
  //                assertThat(persistor().selectBatchBlocking(tx, 3, now.plusMillis(1)),
  // hasSize(2)));
  //  }
  //
  //  @Test
  //  void testBlacklistedExcluded() throws Exception {
  //    txManager()
  //        .inTransactionThrows(
  //            tx -> {
  //              persistor().saveBlocking(tx, createEntry("FOO1", now, false));
  //              persistor().saveBlocking(tx, createEntry("FOO2", now, false));
  //              persistor().saveBlocking(tx, createEntry("FOO3", now, true));
  //            });
  //    txManager()
  //        .inTransactionThrows(
  //            tx ->
  //                assertThat(persistor().selectBatchBlocking(tx, 3, now.plusMillis(1)),
  // hasSize(2)));
  //  }
  //
  //  @Test
  //  void testUpdate() throws Exception {
  //    var entry = createEntry("FOO1", now, false);
  //    txManager().inTransactionThrows(tx -> persistor().saveBlocking(tx, entry));
  //    entry.setAttempts(1);
  //    txManager()
  //        .inTransaction(tx -> assertDoesNotThrow(() -> persistor().updateBlocking(tx, entry)));
  //    entry.setAttempts(2);
  //    txManager()
  //        .inTransaction(tx -> assertDoesNotThrow(() -> persistor().updateBlocking(tx, entry)));
  //    txManager()
  //        .inTransactionThrows(
  //            tx ->
  //                assertThat(
  //                    persistor().selectBatchBlocking(tx, 1, now.plusMillis(1)),
  // contains(entry)));
  //  }
  //
  //  @Test
  //  void testUpdateOptimisticLockFailure() throws Exception {
  //    TransactionOutboxEntry entry = createEntry("FOO1", now, false);
  //    txManager().inTransactionThrows(tx -> persistor().saveBlocking(tx, entry));
  //    TransactionOutboxEntry original = entry.toBuilder().build();
  //    entry.setAttempts(1);
  //    txManager()
  //        .inTransaction(tx -> assertDoesNotThrow(() -> persistor().updateBlocking(tx, entry)));
  //    original.setAttempts(2);
  //    txManager()
  //        .inTransaction(
  //            tx ->
  //                assertThrows(
  //                    OptimisticLockException.class, () -> persistor().updateBlocking(tx,
  // original)));
  //  }
  //
  //  @Test
  //  void testDelete() throws Exception {
  //    TransactionOutboxEntry entry = createEntry("FOO1", now, false);
  //    txManager().inTransactionThrows(tx -> persistor().saveBlocking(tx, entry));
  //    txManager()
  //        .inTransaction(tx -> assertDoesNotThrow(() -> persistor().deleteBlocking(tx, entry)));
  //  }
  //
  //  @Test
  //  void testDeleteOptimisticLockFailure() throws Exception {
  //    TransactionOutboxEntry entry = createEntry("FOO1", now, false);
  //    txManager().inTransactionThrows(tx -> persistor().saveBlocking(tx, entry));
  //    txManager()
  //        .inTransaction(tx -> assertDoesNotThrow(() -> persistor().deleteBlocking(tx, entry)));
  //    txManager()
  //        .inTransaction(
  //            tx ->
  //                assertThrows(
  //                    OptimisticLockException.class, () -> persistor().deleteBlocking(tx,
  // entry)));
  //  }
  //
  //  @Test
  //  void testLock() throws Exception {
  //    TransactionOutboxEntry entry = createEntry("FOO1", now, false);
  //    txManager().inTransactionThrows(tx -> persistor().saveBlocking(tx, entry));
  //    entry.setAttempts(1);
  //    txManager()
  //        .inTransaction(tx -> assertDoesNotThrow(() -> persistor().updateBlocking(tx, entry)));
  //    txManager()
  //        .inTransactionThrows(tx -> assertThat(persistor().lockBlocking(tx, entry),
  // equalTo(true)));
  //  }
  //
  //  @Test
  //  void testLockOptimisticLockFailure() throws Exception {
  //    TransactionOutboxEntry entry = createEntry("FOO1", now, false);
  //    txManager().inTransactionThrows(tx -> persistor().saveBlocking(tx, entry));
  //    TransactionOutboxEntry original = entry.toBuilder().build();
  //    entry.setAttempts(1);
  //    txManager()
  //        .inTransaction(tx -> assertDoesNotThrow(() -> persistor().updateBlocking(tx, entry)));
  //    txManager()
  //        .inTransactionThrows(
  //            tx -> assertThat(persistor().lockBlocking(tx, original), equalTo(false)));
  //  }
  //
  //  @Test
  //  void testSkipLocked() throws Exception {
  //    Assumptions.assumeTrue(dialect().isSupportsSkipLock());
  //
  //    var entry1 = createEntry("FOO1", now.minusSeconds(1), false);
  //    var entry2 = createEntry("FOO2", now.minusSeconds(1), false);
  //    var entry3 = createEntry("FOO3", now.minusSeconds(1), false);
  //    var entry4 = createEntry("FOO4", now.minusSeconds(1), false);
  //
  //    txManager()
  //        .inTransactionThrows(
  //            tx -> {
  //              persistor().saveBlocking(tx, entry1);
  //              persistor().saveBlocking(tx, entry2);
  //              persistor().saveBlocking(tx, entry3);
  //              persistor().saveBlocking(tx, entry4);
  //            });
  //
  //    var gotLockLatch = new CountDownLatch(1);
  //    var executorService = Executors.newFixedThreadPool(1);
  //    try {
  //      Future<?> future =
  //          executorService.submit(
  //              () -> {
  //                log.info("Background thread starting");
  //                txManager()
  //                    .inTransactionThrows(
  //                        tx -> {
  //                          log.info("Background thread attempting select batch");
  //                          var batch = persistor().selectBatchBlocking(tx, 2, now);
  //                          assertThat(batch, hasSize(2));
  //                          log.info("Background thread obtained locks, going to sleep");
  //                          gotLockLatch.countDown();
  //                          expectTobeInterrupted();
  //                          for (TransactionOutboxEntry entry : batch) {
  //                            persistor().delete(tx, entry);
  //                          }
  //                        });
  //                return null;
  //              });
  //
  //      // Wait for the background thread to have obtained the lock
  //      log.info("Waiting for background thread to obtain lock");
  //      assertTrue(gotLockLatch.await(10, TimeUnit.SECONDS));
  //
  //      // Now try and select all four - we should only get two
  //      log.info("Attempting to obtain duplicate locks");
  //      txManager()
  //          .inTransactionThrows(
  //              tx -> {
  //                var batch = persistor().selectBatchBlocking(tx, 4, now);
  //                assertThat(batch, hasSize(2));
  //                for (TransactionOutboxEntry entry : batch) {
  //                  persistor().delete(tx, entry);
  //                }
  //              });
  //
  //      // Kill the other thread
  //      log.info("Shutting down");
  //      future.cancel(true);
  //
  //      // Make sure any assertions from the other thread are propagated
  //      assertThrows(CancellationException.class, future::get);
  //
  //      // Ensure that all the records are processed
  //      txManager()
  //          .inTransactionThrows(
  //              tx -> assertThat(persistor().selectBatchBlocking(tx, 100, now), empty()));
  //
  //    } finally {
  //      executorService.shutdown();
  //      assertTrue(executorService.awaitTermination(10, TimeUnit.SECONDS));
  //    }
  //  }
  //
  //  @Test
  //  void testLockPessimisticLockFailure() throws Exception {
  //    TransactionOutboxEntry entry = createEntry("FOO1", now, false);
  //    txManager().inTransactionThrows(tx -> persistor().saveBlocking(tx, entry));
  //
  //    CountDownLatch gotLockLatch = new CountDownLatch(1);
  //    ExecutorService executorService = Executors.newFixedThreadPool(1);
  //    try {
  //
  //      // Submit another thread which will take a lock and hold it. If it is not
  //      // told to stop after 10 seconds it fails.
  //      Future<?> future =
  //          executorService.submit(
  //              () -> {
  //                log.info("Background thread starting");
  //                txManager()
  //                    .inTransactionThrows(
  //                        tx -> {
  //                          log.info("Background thread attempting lock");
  //                          assertDoesNotThrow(() -> persistor().lockBlocking(tx, entry));
  //                          log.info("Background thread obtained lock, going to sleep");
  //                          gotLockLatch.countDown();
  //                          expectTobeInterrupted();
  //                        });
  //              });
  //
  //      // Wait for the background thread to have obtained the lock
  //      log.info("Waiting for background thread to obtain lock");
  //      assertTrue(gotLockLatch.await(10, TimeUnit.SECONDS));
  //
  //      // Now try and take the lock, which should fail
  //      log.info("Attempting to obtain duplicate lock");
  //      txManager().inTransactionThrows(tx -> assertFalse(persistor().lockBlocking(tx, entry)));
  //
  //      // Kill the other thread
  //      log.info("Shutting down");
  //      future.cancel(true);
  //
  //      // Make sure any assertions from the other thread are propagated
  //      assertThrows(CancellationException.class, future::get);
  //
  //    } finally {
  //      executorService.shutdown();
  //      assertTrue(executorService.awaitTermination(10, TimeUnit.SECONDS));
  //    }
  //  }
  //
  private TransactionOutboxEntry createEntry(
      String id, Instant nextAttemptTime, boolean blacklisted) {
    return TransactionOutboxEntry.builder()
        .id(id)
        .invocation(createInvocation())
        .blacklisted(blacklisted)
        .nextAttemptTime(nextAttemptTime.truncatedTo(MILLIS))
        .build();
  }

  private TransactionOutboxEntry createEntry(
      String id, Instant nextAttemptTime, boolean blacklisted, String uniqueId) {
    return TransactionOutboxEntry.builder()
        .id(id)
        .invocation(createInvocation())
        .blacklisted(blacklisted)
        .nextAttemptTime(nextAttemptTime.truncatedTo(MILLIS))
        .uniqueRequestId(uniqueId)
        .build();
  }

  @NotNull
  private Invocation createInvocation() {
    return new Invocation(
        "Foo",
        "Bar",
        new Class<?>[] {int.class, BigDecimal.class, String.class},
        new Object[] {1, BigDecimal.TEN, null});
  }

  private void expectTobeInterrupted() {
    try {
      Thread.sleep(10000);
      throw new RuntimeException("Background thread not killed within 10 seconds");
    } catch (InterruptedException e) {
      log.info("Background thread interrupted correctly");
    } catch (Exception e) {
      log.error("Background thread failed", e);
      throw e;
    }
  }
}
