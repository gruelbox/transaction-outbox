package com.gruelbox.transactionoutbox;

import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.ea.async.Async;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
public abstract class AbstractSqlPersistorTest<CN, TX extends Transaction<CN, ?>> {

  private Instant now = now();

  protected abstract Dialect dialect();

  protected abstract Persistor<CN, TX> persistor();

  protected abstract TransactionManager<CN, ?, TX> txManager();

  protected void validateState() {}

  @BeforeAll
  static void beforeAll() {
    Async.init();
  }

  @BeforeEach
  void beforeEach() throws InterruptedException, ExecutionException, TimeoutException {
    persistor().migrate(txManager());
    log.info("Validating state");
    validateState();
    log.info("Clearing old records");
    txManager().transactionally(persistor()::clear).get(10, TimeUnit.SECONDS);
  }

  @Test
  void testInsertAndSelect() {
    TransactionOutboxEntry entry = createEntry("FOO", now, false);
    List<TransactionOutboxEntry> entries =
        txManager()
            .transactionally(tx -> persistor().save(tx, entry))
            .thenCompose(
                __ ->
                    txManager()
                        .transactionally(
                            tx -> persistor().selectBatch(tx, 100, now.plusSeconds(1))))
            .join();
    assertThat(entries, hasSize(1));
    assertThat(entries, contains(entry));
  }

  @Test
  void testInsertDuplicate() {
    TransactionOutboxEntry entry1 = createEntry("FOO1", now, false, "context-clientkey1");
    txManager().transactionally(tx -> persistor().save(tx, entry1)).join();
    List<TransactionOutboxEntry> entries =
        txManager()
            .transactionally(tx -> persistor().selectBatch(tx, 100, now.plusSeconds(1)))
            .join();
    assertThat(entries, contains(entry1));

    TransactionOutboxEntry entry2 = createEntry("FOO2", now, false, "context-clientkey2");
    txManager().transactionally(tx -> persistor().save(tx, entry2)).join();
    entries =
        txManager()
            .transactionally(tx -> persistor().selectBatch(tx, 100, now.plusSeconds(1)))
            .join();
    assertThat(entries, containsInAnyOrder(entry1, entry2));

    TransactionOutboxEntry entry3 = createEntry("FOO3", now, false, "context-clientkey1");
    Throwable cause =
        assertThrows(
                CompletionException.class,
                () -> txManager().transactionally(tx -> persistor().save(tx, entry3)).join())
            .getCause();
    assertThat(cause, isA(AlreadyScheduledException.class));

    entries =
        (txManager().transactionally(tx -> persistor().selectBatch(tx, 100, now.plusSeconds(1))))
            .join();
    assertThat(entries, containsInAnyOrder(entry1, entry2));
  }

  @Test
  void testBatchLimitUnderThreshold() {
    List<TransactionOutboxEntry> entries =
        txManager()
            .transactionally(
                tx ->
                    CompletableFuture.allOf(
                        persistor().save(tx, createEntry("FOO1", now, false)),
                        persistor().save(tx, createEntry("FOO2", now, false)),
                        persistor().save(tx, createEntry("FOO3", now, false))))
            .thenCompose(
                __ ->
                    txManager()
                        .transactionally(tx -> persistor().selectBatch(tx, 2, now.plusSeconds(1))))
            .join();
    assertThat(entries, hasSize(2));
  }

  @Test
  void testBatchLimitMatchingThreshold() {
    List<TransactionOutboxEntry> entries =
        txManager()
            .transactionally(
                tx ->
                    CompletableFuture.allOf(
                        persistor().save(tx, createEntry("FOO1", now, false)),
                        persistor().save(tx, createEntry("FOO2", now, false)),
                        persistor().save(tx, createEntry("FOO3", now, false))))
            .thenCompose(
                __ ->
                    txManager()
                        .transactionally(tx -> persistor().selectBatch(tx, 3, now.plusSeconds(1))))
            .join();
    assertThat(entries, hasSize(3));
  }

  @Test
  void testBatchLimitOverThreshold() {
    List<TransactionOutboxEntry> entries =
        txManager()
            .transactionally(
                tx ->
                    CompletableFuture.allOf(
                        persistor().save(tx, createEntry("FOO1", now, false)),
                        persistor().save(tx, createEntry("FOO2", now, false)),
                        persistor().save(tx, createEntry("FOO3", now, false))))
            .thenCompose(
                __ ->
                    txManager()
                        .transactionally(tx -> persistor().selectBatch(tx, 4, now.plusSeconds(1))))
            .join();
    assertThat(entries, hasSize(3));
  }

  @Test
  void testBatchHorizon() {
    List<TransactionOutboxEntry> entries =
        txManager()
            .transactionally(
                tx ->
                    CompletableFuture.allOf(
                        persistor().save(tx, createEntry("FOO1", now, false)),
                        persistor().save(tx, createEntry("FOO2", now, false)),
                        persistor().save(tx, createEntry("FOO3", now.plusSeconds(2), false))))
            .thenCompose(
                __ ->
                    txManager()
                        .transactionally(tx -> persistor().selectBatch(tx, 3, now.plusSeconds(1))))
            .join();
    assertThat(
        entries.stream().map(TransactionOutboxEntry::getId).collect(toList()),
        containsInAnyOrder("FOO1", "FOO2"));
  }

  @Test
  void testBatchOverHorizon() {
    List<TransactionOutboxEntry> entries =
        txManager()
            .transactionally(
                tx ->
                    CompletableFuture.allOf(
                        persistor().save(tx, createEntry("FOO1", now.plusSeconds(2), false)),
                        persistor().save(tx, createEntry("FOO2", now, false)),
                        persistor().save(tx, createEntry("FOO3", now.plusSeconds(2), false))))
            .thenCompose(
                __ ->
                    txManager()
                        .transactionally(tx -> persistor().selectBatch(tx, 3, now.plusSeconds(1))))
            .join();
    assertThat(
        entries.stream().map(TransactionOutboxEntry::getId).collect(toList()),
        containsInAnyOrder("FOO2"));
  }

  @Test
  void testBlacklistedExcluded() {
    List<TransactionOutboxEntry> entries =
        txManager()
            .transactionally(
                tx ->
                    CompletableFuture.allOf(
                        persistor().save(tx, createEntry("FOO1", now, false)),
                        persistor().save(tx, createEntry("FOO2", now, true)),
                        persistor().save(tx, createEntry("FOO3", now, false))))
            .thenCompose(
                __ ->
                    txManager()
                        .transactionally(tx -> persistor().selectBatch(tx, 3, now.plusSeconds(1))))
            .join();
    assertThat(
        entries.stream().map(TransactionOutboxEntry::getId).collect(toList()),
        containsInAnyOrder("FOO1", "FOO3"));
  }

  @Test
  void testUpdate() {
    var entry = createEntry("FOO1", now, false);
    List<TransactionOutboxEntry> entries =
        txManager()
            .transactionally(tx -> persistor().save(tx, entry))
            .thenRun(() -> entry.setAttempts(1))
            .thenCompose(__ -> txManager().transactionally(tx -> persistor().update(tx, entry)))
            .thenRun(() -> entry.setAttempts(2))
            .thenCompose(__ -> txManager().transactionally(tx -> persistor().update(tx, entry)))
            .thenCompose(
                __ ->
                    txManager()
                        .transactionally(tx -> persistor().selectBatch(tx, 1, now.plusSeconds(1))))
            .join();
    assertThat(entries, containsInAnyOrder(entry));
  }

  @Test
  void testUpdateOptimisticLockFailure() {
    var entry = createEntry("FOO1", now, false);
    AtomicReference<TransactionOutboxEntry> original = new AtomicReference<>();
    CompletionException completionException =
        assertThrows(
            CompletionException.class,
            () ->
                txManager()
                    .transactionally(tx -> persistor().save(tx, entry))
                    .thenRun(() -> original.set(entry.toBuilder().build()))
                    .thenRun(() -> entry.setAttempts(1))
                    .thenCompose(
                        __ -> txManager().transactionally(tx -> persistor().update(tx, entry)))
                    .thenRun(() -> original.get().setAttempts(2))
                    .thenCompose(
                        __ ->
                            txManager()
                                .transactionally(tx -> persistor().update(tx, original.get())))
                    .join());
    assertThat(completionException.getCause(), isA(OptimisticLockException.class));
  }

  @Test
  void testDelete() {
    var entry = createEntry("FOO1", now, false);
    List<TransactionOutboxEntry> entries =
        txManager()
            .transactionally(tx -> persistor().save(tx, entry))
            .thenCompose(__ -> txManager().transactionally(tx -> persistor().delete(tx, entry)))
            .thenCompose(
                __ ->
                    txManager()
                        .transactionally(tx -> persistor().selectBatch(tx, 1, now.plusSeconds(1))))
            .join();
    assertThat(entries, empty());
  }

  @Test
  void testDeleteOptimisticLockFailure() {
    var entry = createEntry("FOO1", now, false);
    CompletionException completionException =
        assertThrows(
            CompletionException.class,
            () ->
                txManager()
                    .transactionally(tx -> persistor().save(tx, entry))
                    .thenCompose(
                        __ -> txManager().transactionally(tx -> persistor().delete(tx, entry)))
                    .thenCompose(
                        __ -> txManager().transactionally(tx -> persistor().delete(tx, entry)))
                    .join());
    assertThat(completionException.getCause(), isA(OptimisticLockException.class));
  }

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
