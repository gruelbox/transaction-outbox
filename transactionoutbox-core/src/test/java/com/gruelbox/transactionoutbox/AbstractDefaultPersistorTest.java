package com.gruelbox.transactionoutbox;

import static java.time.Instant.now;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.Instant;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
abstract class AbstractDefaultPersistorTest {

  private Instant now = now();

  protected abstract DefaultPersistor persistor();

  protected abstract TransactionManager txManager();

  @BeforeEach
  void beforeAll() throws SQLException {
    persistor().migrate(txManager());
    txManager().inTransactionThrows(persistor()::clear);
  }

  @Test
  void testInsertAndSelect() throws Exception {
    TransactionOutboxEntry entry = createEntry("FOO", now, false);
    txManager().inTransactionThrows(tx -> persistor().save(tx, entry));
    Thread.sleep(1100);
    txManager()
        .inTransactionThrows(
            tx -> assertThat(persistor().selectBatch(tx, 100, now.plusMillis(1)), contains(entry)));
  }

  @Test
  void testInsertDuplicate() throws Exception {
    TransactionOutboxEntry entry1 = createEntry("FOO1", now, false, "context-clientkey1");
    txManager().inTransactionThrows(tx -> persistor().save(tx, entry1));
    Thread.sleep(1100);
    txManager()
        .inTransactionThrows(
            tx ->
                assertThat(persistor().selectBatch(tx, 100, now.plusMillis(1)), contains(entry1)));

    TransactionOutboxEntry entry2 = createEntry("FOO2", now, false, "context-clientkey2");
    txManager().inTransactionThrows(tx -> persistor().save(tx, entry2));
    Thread.sleep(1100);
    txManager()
        .inTransactionThrows(
            tx ->
                assertThat(
                    persistor().selectBatch(tx, 100, now.plusMillis(1)),
                    containsInAnyOrder(entry1, entry2)));

    TransactionOutboxEntry entry3 = createEntry("FOO3", now, false, "context-clientkey1");
    Assertions.assertThrows(
        AlreadyScheduledException.class,
        () -> txManager().inTransactionThrows(tx -> persistor().save(tx, entry3)));
    txManager()
        .inTransactionThrows(
            tx ->
                assertThat(
                    persistor().selectBatch(tx, 100, now.plusMillis(1)),
                    containsInAnyOrder(entry1, entry2)));
  }

  @Test
  void testBatchLimitUnderThreshold() throws Exception {
    txManager()
        .inTransactionThrows(
            tx -> {
              persistor().save(tx, createEntry("FOO1", now, false));
              persistor().save(tx, createEntry("FOO2", now, false));
              persistor().save(tx, createEntry("FOO3", now, false));
            });
    txManager()
        .inTransactionThrows(
            tx -> assertThat(persistor().selectBatch(tx, 2, now.plusMillis(1)), hasSize(2)));
  }

  @Test
  void testBatchLimitMatchingThreshold() throws Exception {
    txManager()
        .inTransactionThrows(
            tx -> {
              persistor().save(tx, createEntry("FOO1", now, false));
              persistor().save(tx, createEntry("FOO2", now, false));
              persistor().save(tx, createEntry("FOO3", now, false));
            });
    txManager()
        .inTransactionThrows(
            tx -> assertThat(persistor().selectBatch(tx, 3, now.plusMillis(1)), hasSize(3)));
  }

  @Test
  void testBatchLimitOverThreshold() throws Exception {
    txManager()
        .inTransactionThrows(
            tx -> {
              persistor().save(tx, createEntry("FOO1", now, false));
              persistor().save(tx, createEntry("FOO2", now, false));
              persistor().save(tx, createEntry("FOO3", now, false));
            });
    txManager()
        .inTransactionThrows(
            tx -> assertThat(persistor().selectBatch(tx, 4, now.plusMillis(1)), hasSize(3)));
  }

  @Test
  void testBatchHorizon() throws Exception {
    txManager()
        .inTransactionThrows(
            tx -> {
              persistor().save(tx, createEntry("FOO1", now, false));
              persistor().save(tx, createEntry("FOO2", now, false));
              persistor().save(tx, createEntry("FOO3", now.plusMillis(2), false));
            });
    txManager()
        .inTransactionThrows(
            tx -> assertThat(persistor().selectBatch(tx, 3, now.plusMillis(1)), hasSize(2)));
  }

  @Test
  void testBlacklistedExcluded() throws Exception {
    txManager()
        .inTransactionThrows(
            tx -> {
              persistor().save(tx, createEntry("FOO1", now, false));
              persistor().save(tx, createEntry("FOO2", now, false));
              persistor().save(tx, createEntry("FOO3", now, true));
            });
    txManager()
        .inTransactionThrows(
            tx -> assertThat(persistor().selectBatch(tx, 3, now.plusMillis(1)), hasSize(2)));
  }

  @Test
  void testUpdate() throws Exception {
    var entry = createEntry("FOO1", now, false);
    txManager().inTransactionThrows(tx -> persistor().save(tx, entry));
    entry.setAttempts(1);
    txManager().inTransaction(tx -> assertDoesNotThrow(() -> persistor().update(tx, entry)));
    entry.setAttempts(2);
    txManager().inTransaction(tx -> assertDoesNotThrow(() -> persistor().update(tx, entry)));
    txManager()
        .inTransactionThrows(
            tx -> assertThat(persistor().selectBatch(tx, 1, now.plusMillis(1)), contains(entry)));
  }

  @Test
  void testUpdateOptimisticLockFailure() throws Exception {
    TransactionOutboxEntry entry = createEntry("FOO1", now, false);
    txManager().inTransactionThrows(tx -> persistor().save(tx, entry));
    TransactionOutboxEntry original = entry.toBuilder().build();
    entry.setAttempts(1);
    txManager().inTransaction(tx -> assertDoesNotThrow(() -> persistor().update(tx, entry)));
    original.setAttempts(2);
    txManager()
        .inTransaction(
            tx ->
                assertThrows(
                    OptimisticLockException.class, () -> persistor().update(tx, original)));
  }

  @Test
  void testDelete() throws Exception {
    TransactionOutboxEntry entry = createEntry("FOO1", now, false);
    txManager().inTransactionThrows(tx -> persistor().save(tx, entry));
    txManager().inTransaction(tx -> assertDoesNotThrow(() -> persistor().delete(tx, entry)));
  }

  @Test
  void testDeleteOptimisticLockFailure() throws Exception {
    TransactionOutboxEntry entry = createEntry("FOO1", now, false);
    txManager().inTransactionThrows(tx -> persistor().save(tx, entry));
    txManager().inTransaction(tx -> assertDoesNotThrow(() -> persistor().delete(tx, entry)));
    txManager()
        .inTransaction(
            tx -> assertThrows(OptimisticLockException.class, () -> persistor().delete(tx, entry)));
  }

  @Test
  void testLock() throws Exception {
    TransactionOutboxEntry entry = createEntry("FOO1", now, false);
    txManager().inTransactionThrows(tx -> persistor().save(tx, entry));
    entry.setAttempts(1);
    txManager().inTransaction(tx -> assertDoesNotThrow(() -> persistor().update(tx, entry)));
    txManager().inTransactionThrows(tx -> assertThat(persistor().lock(tx, entry), equalTo(true)));
  }

  @Test
  void testLockOptimisticLockFailure() throws Exception {
    TransactionOutboxEntry entry = createEntry("FOO1", now, false);
    txManager().inTransactionThrows(tx -> persistor().save(tx, entry));
    TransactionOutboxEntry original = entry.toBuilder().build();
    entry.setAttempts(1);
    txManager().inTransaction(tx -> assertDoesNotThrow(() -> persistor().update(tx, entry)));
    txManager()
        .inTransactionThrows(tx -> assertThat(persistor().lock(tx, original), equalTo(false)));
  }

  @Test
  void testLockPessimisticLockFailure() throws Exception {
    TransactionOutboxEntry entry = createEntry("FOO1", now, false);
    txManager().inTransactionThrows(tx -> persistor().save(tx, entry));

    CountDownLatch gotLockLatch = new CountDownLatch(1);
    ExecutorService executorService = Executors.newFixedThreadPool(1);
    try {

      // Submit another thread which will take a lock and hold it. If it is not
      // told to stop after 10 seconds it fails.
      Future<?> future =
          executorService.submit(
              () -> {
                log.info("Background thread starting");
                txManager()
                    .inTransactionThrows(
                        tx -> {
                          log.info("Background thread attempting lock");
                          assertDoesNotThrow(() -> persistor().lock(tx, entry));
                          log.info("Background thread obtained lock, going to sleep");
                          gotLockLatch.countDown();
                          try {
                            Thread.sleep(10000);
                            throw new RuntimeException(
                                "Background thread not killed within 10 seconds");
                          } catch (InterruptedException e) {
                            log.info("Background thread interrupted correctly");
                          } catch (Exception e) {
                            log.error("Background thread failed", e);
                            throw e;
                          }
                        });
              });

      // Wait for the background thread to have obtained the lock
      log.info("Waiting for background thread to obtain lock");
      assertTrue(gotLockLatch.await(10, TimeUnit.SECONDS));

      // Now try and take the lock, which should fail
      log.info("Attempting to obtain duplicate lock");
      txManager().inTransactionThrows(tx -> assertFalse(persistor().lock(tx, entry)));

      // Kill the other thread
      log.info("Shutting down");
      future.cancel(true);

      // Make sure any assertions from the other thread are propagated
      assertThrows(CancellationException.class, future::get);

    } finally {
      executorService.shutdown();
      assertTrue(executorService.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private TransactionOutboxEntry createEntry(
      String id, Instant nextAttemptTime, boolean blacklisted) {
    return TransactionOutboxEntry.builder()
        .id(id)
        .invocation(
            new Invocation(
                "Foo",
                "Bar",
                new Class<?>[] {int.class, BigDecimal.class, String.class},
                new Object[] {1, BigDecimal.TEN, null}))
        .blacklisted(blacklisted)
        .nextAttemptTime(nextAttemptTime)
        .build();
  }

  private TransactionOutboxEntry createEntry(
      String id, Instant nextAttemptTime, boolean blacklisted, String uniqueId) {
    return TransactionOutboxEntry.builder()
        .id(id)
        .invocation(
            new Invocation(
                "Foo",
                "Bar",
                new Class<?>[] {int.class, BigDecimal.class, String.class},
                new Object[] {1, BigDecimal.TEN, null}))
        .blacklisted(blacklisted)
        .nextAttemptTime(nextAttemptTime)
        .uniqueRequestId(uniqueId)
        .build();
  }
}
