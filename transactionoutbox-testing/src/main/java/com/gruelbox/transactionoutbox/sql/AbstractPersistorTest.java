package com.gruelbox.transactionoutbox.sql;

import static com.gruelbox.transactionoutbox.Utils.sneakyThrowing;
import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gruelbox.transactionoutbox.AlreadyScheduledException;
import com.gruelbox.transactionoutbox.Invocation;
import com.gruelbox.transactionoutbox.OptimisticLockException;
import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.Utils;
import com.gruelbox.transactionoutbox.spi.BaseTransaction;
import com.gruelbox.transactionoutbox.spi.BaseTransactionManager;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Any implementation of {@link Persistor} should be accompanied by a subclass of this test to
 * ensure that it delivers against the contract.
 *
 * @param <CN> The connection type.
 * @param <TX> The transaction type.
 */
@Slf4j
public abstract class AbstractPersistorTest<CN, TX extends BaseTransaction<CN>> {

  private Instant now = now().truncatedTo(SECONDS);

  protected abstract Dialect dialect();

  protected abstract Persistor<CN, TX> persistor();

  protected abstract BaseTransactionManager<CN, TX> txManager();

  protected void validateState() {}

  @BeforeEach
  void beforeEach() throws InterruptedException, ExecutionException, TimeoutException {
    CompletableFuture<Boolean> checkConnectionResult =
        txManager().transactionally(tx -> persistor().checkConnection(tx));
    assertTrue(Utils.join(checkConnectionResult));
    persistor().migrate(txManager());
    log.info("Validating state");
    validateState();
    log.info("Clearing old records");
    txManager().transactionally(persistor()::clear).get(10, TimeUnit.SECONDS);
    log.info("Cleared");
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
  void testInsertWithUniqueRequestIdFailureBubblesExceptionUp() {
    var invalidEntry =
        createEntry("FOO", now, false).toBuilder()
            .uniqueRequestId("INTENTIONALLY_TOO_LONG_TO_CAUSE_BLOW_UP".repeat(10))
            .build();
    assertThrows(
        RuntimeException.class,
        () -> Utils.join(txManager().transactionally(tx -> persistor().save(tx, invalidEntry))));
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
    assertThrows(
        AlreadyScheduledException.class,
        () ->
            sneakyThrowing(
                () -> txManager().transactionally(tx -> persistor().save(tx, entry3)).join()));

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
  void testDeleteProcessedAndExpired() {
    var entry1 = createEntry("FOO1", now, false);
    var entry2 = createEntry("FOO2", now, false);
    entry2.setProcessed(true);
    var entry3 = createEntry("FOO3", now.plusSeconds(5), false);
    entry3.setProcessed(true);
    var entry4 = createEntry("FOO4", now.plusSeconds(3), false);
    entry4.setProcessed(true);

    txManager()
        .transactionally(
            tx ->
                CompletableFuture.allOf(
                    persistor().save(tx, entry1),
                    persistor().save(tx, entry2),
                    persistor().save(tx, entry3),
                    persistor().save(tx, entry4)))
        .join();

    Integer records =
        txManager()
            .transactionally(
                tx -> persistor().deleteProcessedAndExpired(tx, 10, now.plusSeconds(4)))
            .join();
    assertThat(records, equalTo(2));

    entry2.setProcessed(false);
    entry3.setProcessed(false);
    entry4.setProcessed(false);
    assertThrows(
        OptimisticLockException.class,
        () ->
            sneakyThrowing(
                () -> txManager().transactionally(tx -> persistor().update(tx, entry2)).join()));
    assertThrows(
        OptimisticLockException.class,
        () ->
            sneakyThrowing(
                () -> txManager().transactionally(tx -> persistor().update(tx, entry4)).join()));

    assertDoesNotThrow(
        () -> txManager().transactionally(tx -> persistor().update(tx, entry3)).join());

    List<TransactionOutboxEntry> entries =
        txManager()
            .transactionally(tx -> persistor().selectBatch(tx, 10, now.plusSeconds(10)))
            .join();

    assertThat(
        entries.stream().map(TransactionOutboxEntry::getId).collect(toList()),
        containsInAnyOrder("FOO1", "FOO3"));
  }

  static class TransactionOutboxEntryMatcher extends TypeSafeMatcher<TransactionOutboxEntry> {
    private final TransactionOutboxEntry entry;

    TransactionOutboxEntryMatcher(TransactionOutboxEntry entry) {
      this.entry = entry;
    }

    @Override
    protected boolean matchesSafely(TransactionOutboxEntry other) {
      return entry.getId().equals(other.getId())
          && entry.getInvocation().equals(other.getInvocation())
          && entry.getNextAttemptTime().equals(other.getNextAttemptTime())
          && entry.getAttempts() == other.getAttempts()
          && entry.getVersion() == other.getVersion()
          && entry.isBlocked() == other.isBlocked()
          && entry.isProcessed() == other.isProcessed();
    }

    @Override
    public void describeTo(Description description) {
      description
          .appendText("Should match on all fields outside of lastAttemptTime :")
          .appendText(entry.toString());
    }
  }

  TransactionOutboxEntryMatcher matches(TransactionOutboxEntry e) {
    return new TransactionOutboxEntryMatcher(e);
  }

  @Test
  void testUpdate() {
    var entry = createEntry("FOO1", now, false);
    List<TransactionOutboxEntry> entries =
        txManager()
            .transactionally(tx -> persistor().save(tx, entry))
            .thenRun(() -> entry.setAttempts(1))
            .thenCompose(__ -> txManager().transactionally(tx -> persistor().update(tx, entry)))
            .thenCompose(
                __ ->
                    txManager()
                        .transactionally(tx -> persistor().selectBatch(tx, 1, now.plusMillis(1))))
            .thenAccept(
                found -> {
                  assertThat(found.size(), equalTo(1));
                  assertThat(found.get(0), matches(entry));
                  assertThat(found.get(0).getLastAttemptTime(), nullValue());
                })
            .thenRun(
                () -> {
                  entry.setAttempts(2);
                  entry.setLastAttemptTime(now);
                })
            .thenCompose(__ -> txManager().transactionally(tx -> persistor().update(tx, entry)))
            .thenCompose(
                __ ->
                    txManager()
                        .transactionally(tx -> persistor().selectBatch(tx, 1, now.plusSeconds(1))))
            .join();
    assertThat(entries.size(), equalTo(1));
    assertThat(entries.get(0), matches(entry));
    assertThat(entries.get(0).getLastAttemptTime(), notNullValue());
  }

  @Test
  void testUnblock() {
    var entry1 = createEntry("FOOx", now, false);
    var entry2 = createEntry("FOOy", now, false);
    entry2.setBlocked(true);
    entry2.setAttempts(3);
    txManager().transactionally(tx -> persistor().save(tx, entry1)).join();
    txManager().transactionally(tx -> persistor().save(tx, entry2)).join();
    assertFalse(txManager().transactionally(tx -> persistor().unblock(tx, "FOOx")).join());
    assertTrue(txManager().transactionally(tx -> persistor().unblock(tx, "FOOy")).join());
    assertFalse(txManager().transactionally(tx -> persistor().unblock(tx, "FOOy")).join());
    entry2.setBlocked(false);
    entry2.setVersion(entry2.getVersion() + 1);
    entry2.setAttempts(0);
    List<TransactionOutboxEntry> entries =
        txManager()
            .transactionally(tx -> persistor().selectBatch(tx, 10, now.plusSeconds(1)))
            .join();
    assertThat(entries, containsInAnyOrder(entry1, entry2));
  }

  @Test
  void testUpdateOptimisticLockFailure() {
    var entry = createEntry("FOO1", now, false);
    AtomicReference<TransactionOutboxEntry> original = new AtomicReference<>();
    assertThrows(
        OptimisticLockException.class,
        () ->
            sneakyThrowing(
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
                        .join()));
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

  @Test
  void testLock() {
    var entry = createEntry("FOO1", now, false);
    Boolean lockSuccess =
        txManager()
            .transactionally(tx -> persistor().save(tx, entry))
            .thenRun(() -> entry.setAttempts(1))
            .thenCompose(__ -> txManager().transactionally(tx -> persistor().update(tx, entry)))
            .thenCompose(__ -> txManager().transactionally(tx -> persistor().lock(tx, entry)))
            .join();
    assertThat(lockSuccess, equalTo(true));
  }

  @Test
  void testLockOptimisticLockFailure() {
    var entry = createEntry("FOO1", now, false);
    AtomicReference<TransactionOutboxEntry> original = new AtomicReference<>();
    boolean lockSuccess =
        txManager()
            .transactionally(tx -> persistor().save(tx, entry))
            .thenRun(() -> original.set(entry.toBuilder().build()))
            .thenRun(() -> entry.setAttempts(1))
            .thenCompose(__ -> txManager().transactionally(tx -> persistor().update(tx, entry)))
            .thenCompose(
                __ -> txManager().transactionally(tx -> persistor().lock(tx, original.get())))
            .join();
    assertThat(lockSuccess, equalTo(false));
  }

  @Test
  void testSkipLocked() throws InterruptedException, TimeoutException, ExecutionException {
    Assumptions.assumeTrue(dialect().isSupportsSkipLock());

    var entry1 = createEntry("FOO1", now.minusSeconds(1), false);
    var entry2 = createEntry("FOO2", now.minusSeconds(1), false);
    var entry3 = createEntry("FOO3", now.minusSeconds(1), false);
    var entry4 = createEntry("FOO4", now.minusSeconds(1), false);

    txManager()
        .transactionally(
            tx ->
                CompletableFuture.allOf(
                    persistor().save(tx, entry1),
                    persistor().save(tx, entry2),
                    persistor().save(tx, entry3),
                    persistor().save(tx, entry4)))
        .join();

    var gotLockLatch = new CountDownLatch(1);
    var releaseLatch = new CountDownLatch(1);
    AtomicReference<List<TransactionOutboxEntry>> locked = new AtomicReference<>();

    // Use a background thread to run the locking so it works for JDBC
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    try {

      Future<?> background =
          executorService.submit(
              () ->
                  txManager()
                      .transactionally(
                          tx ->
                              persistor()
                                  .selectBatch(tx, 2, now)
                                  .thenAccept(
                                      batch -> {
                                        assertThat(batch, hasSize(2));
                                        log.info("Transaction thread obtained locks");
                                        locked.set(batch);
                                      })
                                  .thenCompose(
                                      batch ->
                                          CompletableFuture.supplyAsync(
                                              () -> {
                                                gotLockLatch.countDown();
                                                log.info("Waiter thread going to sleep");
                                                try {
                                                  assertTrue(
                                                      releaseLatch.await(20, TimeUnit.SECONDS));
                                                } catch (InterruptedException e) {
                                                  Thread.currentThread().interrupt();
                                                  throw new RuntimeException(e);
                                                }
                                                return batch;
                                              },
                                              executorService))
                                  .thenCompose(
                                      __ ->
                                          CompletableFuture.allOf(
                                              locked.get().stream()
                                                  .map(entry -> persistor().delete(tx, entry))
                                                  .toArray(CompletableFuture[]::new))))
                      .join());

      // Wait for the other thread to have obtained the lock
      log.info("Waiting for background thread to obtain lock");
      assertTrue(gotLockLatch.await(10, TimeUnit.SECONDS));

      // Now try and select all four - we should only get two
      log.info("Attempting to obtain duplicate locks");
      txManager()
          .transactionally(
              tx ->
                  persistor()
                      .selectBatch(tx, 4, now)
                      .thenCompose(
                          batch -> {
                            assertThat(batch, hasSize(2));
                            return CompletableFuture.allOf(
                                batch.stream()
                                    .map(entry -> persistor().delete(tx, entry))
                                    .toArray(CompletableFuture[]::new));
                          }));

      // Kill the other thread
      log.info("Shutting down");
      releaseLatch.countDown();

      // Make sure any assertions from the other thread are propagated
      background.get(10, TimeUnit.SECONDS);

      // Ensure that all the records are processed
      List<TransactionOutboxEntry> finalRecords =
          txManager().transactionally(tx -> persistor().selectBatch(tx, 100, now)).join();
      assertThat(finalRecords, empty());

    } finally {
      executorService.shutdown();
      assertTrue(executorService.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  @Disabled
  void testLockPessimisticLockFailure()
      throws InterruptedException, TimeoutException, ExecutionException {
    var entry = createEntry("FOO1", now.minusSeconds(1), false);

    txManager().transactionally(tx -> persistor().save(tx, entry)).join();

    var gotLockLatch = new CountDownLatch(1);
    var releaseLatch = new CountDownLatch(1);

    // Use a background thread to run the locking so it works for JDBC
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    try {

      Future<?> background =
          executorService.submit(
              () ->
                  txManager()
                      .transactionally(
                          tx -> {
                            log.info("Start of background transaction on {}", tx.connection());
                            return persistor()
                                .lock(tx, entry)
                                .whenComplete((v, e) -> log.info("Lock result: {}", v))
                                .thenAccept(Assertions::assertTrue)
                                .thenRun(
                                    () ->
                                        log.info(
                                            "Background thread obtained lock on {}",
                                            tx.connection()))
                                .thenCompose(
                                    batch ->
                                        CompletableFuture.supplyAsync(
                                            () -> {
                                              gotLockLatch.countDown();
                                              log.info("Waiter thread going to sleep");
                                              try {
                                                assertTrue(
                                                    releaseLatch.await(20, TimeUnit.SECONDS),
                                                    "Background thread not released in time");
                                              } catch (InterruptedException e) {
                                                Thread.currentThread().interrupt();
                                                throw new RuntimeException(e);
                                              }
                                              return batch;
                                            },
                                            executorService))
                                .thenRun(
                                    () ->
                                        log.info(
                                            "End of background transaction on {}",
                                            tx.connection()));
                          })
                      .join());

      // Wait for the other thread to have obtained the lock
      log.info("Waiting for background thread to obtain lock");
      assertTrue(gotLockLatch.await(10, TimeUnit.SECONDS));

      // Now try and obtain the lock
      log.info("Attempting to obtain duplicate lock");
      boolean locked =
          txManager()
              .transactionally(
                  tx ->
                      persistor()
                          .lock(tx, entry)
                          .thenApply(
                              it -> {
                                log.info("Foreground thread obtained lock: {}", it);
                                return it;
                              }))
              .join();
      log.info("Lock obtained: {}", locked);

      // Kill the other thread
      log.info("Shutting down");
      releaseLatch.countDown();

      // Make sure any assertions from the other thread are propagated
      background.get(10, TimeUnit.SECONDS);

      // Assert lock failed
      assertFalse(locked);

    } finally {
      executorService.shutdown();
      assertTrue(executorService.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private TransactionOutboxEntry createEntry(String id, Instant nextAttemptTime, boolean blocked) {
    return TransactionOutboxEntry.builder()
        .id(id)
        .invocation(createInvocation())
        .blocked(blocked)
        .nextAttemptTime(nextAttemptTime)
        .build();
  }

  @SuppressWarnings("SameParameterValue")
  private TransactionOutboxEntry createEntry(
      String id, Instant nextAttemptTime, boolean blocked, String uniqueId) {
    return TransactionOutboxEntry.builder()
        .id(id)
        .invocation(createInvocation())
        .blocked(blocked)
        .nextAttemptTime(nextAttemptTime)
        .uniqueRequestId(uniqueId)
        .build();
  }

  private Invocation createInvocation() {
    return new Invocation(
        "Foo",
        "Bar",
        new Class<?>[] {int.class, BigDecimal.class, String.class},
        new Object[] {1, BigDecimal.TEN, null});
  }
}
