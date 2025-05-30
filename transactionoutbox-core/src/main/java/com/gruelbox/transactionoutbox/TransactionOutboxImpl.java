package com.gruelbox.transactionoutbox;

import static com.gruelbox.transactionoutbox.spi.Utils.logAtLevel;
import static com.gruelbox.transactionoutbox.spi.Utils.uncheckedly;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.MINUTES;

import com.gruelbox.transactionoutbox.spi.ProxyFactory;
import com.gruelbox.transactionoutbox.spi.Utils;
import java.lang.reflect.InvocationTargetException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.slf4j.event.Level;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
final class TransactionOutboxImpl implements TransactionOutbox, Validatable {

  private final TransactionManager transactionManager;
  private final Persistor persistor;
  private final Instantiator instantiator;
  private final Submitter submitter;
  private final Duration attemptFrequency;
  private final Level logLevelTemporaryFailure;
  private final int blockAfterAttempts;
  private final int flushBatchSize;
  private final Supplier<Clock> clockProvider;
  private final TransactionOutboxListener listener;
  private final boolean serializeMdc;
  private final Validator validator;
  private final Duration retentionThreshold;
  private final boolean enableOrderedBatchProcessing;
  private final AtomicBoolean initialized = new AtomicBoolean();
  private final ProxyFactory proxyFactory = new ProxyFactory();
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

  @Override
  public void validate(Validator validator) {
    validator.notNull("transactionManager", transactionManager);
    validator.valid("persistor", persistor);
    validator.valid("instantiator", instantiator);
    validator.valid("submitter", submitter);
    validator.notNull("attemptFrequency", attemptFrequency);
    validator.notNull("logLevelTemporaryFailure", logLevelTemporaryFailure);
    validator.min("blockAfterAttempts", blockAfterAttempts, 1);
    validator.min("flushBatchSize", flushBatchSize, 1);
    validator.notNull("clockProvider", clockProvider);
    validator.notNull("listener", listener);
    validator.notNull("retentionThreshold", retentionThreshold);
  }

  static TransactionOutboxBuilder builder() {
    return new TransactionOutboxBuilderImpl();
  }

  @Override
  public void initialize() {
    if (initialized.compareAndSet(false, true)) {
      try {
        persistor.migrate(transactionManager);
      } catch (Exception e) {
        initialized.set(false);
        throw e;
      }
    }
  }

  @Override
  public <T> T schedule(Class<T> clazz) {
    return schedule(clazz, null, null, null);
  }

  @Override
  public ParameterizedScheduleBuilder with() {
    return new ParameterizedScheduleBuilderImpl();
  }

  private boolean doFlush(Function<Transaction, Collection<TransactionOutboxEntry>> batchSource) {
    var batch =
        transactionManager.inTransactionReturns(
            transaction -> {
              var entries = batchSource.apply(transaction);
              List<TransactionOutboxEntry> result = new ArrayList<>(entries.size());
              for (var entry : entries) {
                log.debug("Triggering {}", entry.description());
                try {
                  pushBack(transaction, entry);
                  result.add(entry);
                } catch (OptimisticLockException e) {
                  log.debug("Beaten to optimistic lock on {}", entry.description());
                }
              }
              return result;
            });
    log.debug("Got batch of {}", batch.size());
    batch.forEach(this::submitNow);
    log.debug("Submitted batch");
    return !batch.isEmpty();
  }

  private boolean doBatchFlush(
      Function<Transaction, Collection<TransactionOutboxEntry>> batchSource, Executor executor) {
    var batch =
        transactionManager.inTransactionReturns(
            transaction -> {
              var entries = batchSource.apply(transaction);
              List<TransactionOutboxEntry> result = new ArrayList<>(entries.size());
              for (var entry : entries) {
                log.debug("Triggering {}", entry.description());
                entry.setLastAttemptTime(clockProvider.get().instant());
                entry.setNextAttemptTime(after(attemptFrequency));
                validator.validate(entry);
                result.add(entry);
              }

              try {
                persistor.updateBatch(transaction, result);
              } catch (OptimisticLockException e) {
                log.debug("Beaten to optimistic lock");
              } catch (Exception e) {
                Utils.uncheckAndThrow(e);
              }

              return result;
            });

    log.debug("Got batch of {}", batch.size());
    if (!batch.isEmpty()) {
      // Group items by topic
      Map<String, List<TransactionOutboxEntry>> groupedByTopic =
          batch.stream()
              .collect(
                  Collectors.groupingBy(entry -> entry.getTopic() != null ? entry.getTopic() : ""));

      // Process each topic group in parallel
      List<CompletableFuture<Void>> futures = new ArrayList<>();
      groupedByTopic.forEach(
          (topic, entries) -> {
            log.debug("Submitting {} entries for topic {}", entries.size(), topic);
            futures.add(CompletableFuture.runAsync(() -> processBatchNow(entries), executor));
          });

      // Wait for all batches to complete
      if (!futures.isEmpty()) {
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
      }

      log.debug("Processed all batch groups");
    }
    return !batch.isEmpty();
  }

  @Override
  public boolean flush(Executor executor) {
    if (!initialized.get()) {
      throw new IllegalStateException("Not initialized");
    }
    Instant now = clockProvider.get().instant();
    List<CompletableFuture<Boolean>> futures = new ArrayList<>();

    futures.add(
        CompletableFuture.supplyAsync(
            () -> {
              log.debug("Flushing stale tasks");
              return doFlush(
                  tx -> uncheckedly(() -> persistor.selectBatch(tx, flushBatchSize, now)));
            },
            executor));

    futures.add(
        CompletableFuture.runAsync(() -> expireIdempotencyProtection(now), executor)
            .thenApply(it -> false));

    if (enableOrderedBatchProcessing) {
      futures.add(
          CompletableFuture.supplyAsync(
              () -> {
                log.debug("Flushing topics in batches");
                return doBatchFlush(
                    tx ->
                        uncheckedly(
                            () -> persistor.selectNextBatchInTopics(tx, flushBatchSize, now)),
                    executor);
              },
              executor));
    } else {
      futures.add(
          CompletableFuture.supplyAsync(
              () -> {
                log.debug("Flushing topics without batching");
                return doFlush(
                    tx -> uncheckedly(() -> persistor.selectNextInTopics(tx, flushBatchSize, now)));
              },
              executor));
    }

    return futures.stream()
        .reduce((f1, f2) -> f1.thenCombine(f2, (d1, d2) -> d1 || d2))
        .map(CompletableFuture::join)
        .orElse(false);
  }

  @Override
  public boolean flushTopics(Executor executor, List<String> topicNames) {
    if (!initialized.get()) {
      throw new IllegalStateException("Not initialized");
    }
    Instant now = clockProvider.get().instant();

    log.debug("Flushing selected topics {}", topicNames);
    return doFlush(
        tx ->
            uncheckedly(
                () -> persistor.selectNextInSelectedTopics(tx, topicNames, flushBatchSize, now)));
  }

  private void expireIdempotencyProtection(Instant now) {
    long totalRecordsDeleted = 0;
    int recordsDeleted;
    do {
      recordsDeleted =
          transactionManager.inTransactionReturns(
              tx ->
                  uncheckedly(() -> persistor.deleteProcessedAndExpired(tx, flushBatchSize, now)));
      totalRecordsDeleted += recordsDeleted;
    } while (recordsDeleted > 0);
    if (totalRecordsDeleted > 0) {
      String duration =
          String.format(
              "%dd:%02dh:%02dm:%02ds",
              retentionThreshold.toDaysPart(),
              retentionThreshold.toHoursPart(),
              retentionThreshold.toMinutesPart(),
              retentionThreshold.toSecondsPart());
      log.info(
          "Expired idempotency protection on {} requests completed more than {} ago",
          totalRecordsDeleted,
          duration);
    } else {
      log.debug("No records found to delete as of {}", now);
    }
  }

  @Override
  public boolean unblock(String entryId) {
    if (!initialized.get()) {
      throw new IllegalStateException("Not initialized");
    }
    if (!(transactionManager instanceof ThreadLocalContextTransactionManager)) {
      throw new UnsupportedOperationException(
          "This method requires a ThreadLocalContextTransactionManager");
    }
    log.info("Unblocking entry {} for retry.", entryId);
    try {
      return ((ThreadLocalContextTransactionManager) transactionManager)
          .requireTransactionReturns(tx -> persistor.unblock(tx, entryId));
    } catch (Exception e) {
      throw (RuntimeException) Utils.uncheckAndThrow(e);
    }
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public boolean unblock(String entryId, Object transactionContext) {
    if (!initialized.get()) {
      throw new IllegalStateException("Not initialized");
    }
    if (!(transactionManager instanceof ParameterContextTransactionManager)) {
      throw new UnsupportedOperationException(
          "This method requires a ParameterContextTransactionManager");
    }
    log.info("Unblocking entry {} for retry", entryId);
    try {
      if (transactionContext instanceof Transaction) {
        return persistor.unblock((Transaction) transactionContext, entryId);
      }
      Transaction transaction =
          ((ParameterContextTransactionManager) transactionManager)
              .transactionFromContext(transactionContext);
      return persistor.unblock(transaction, entryId);
    } catch (Exception e) {
      throw (RuntimeException) Utils.uncheckAndThrow(e);
    }
  }

  private <T> T schedule(
      Class<T> clazz, String uniqueRequestId, String topic, Duration delayForAtLeast) {
    if (!initialized.get()) {
      throw new IllegalStateException("Not initialized");
    }
    return proxyFactory.createProxy(
        clazz,
        (method, args) ->
            uncheckedly(
                () -> {
                  var extracted = transactionManager.extractTransaction(method, args);
                  TransactionOutboxEntry entry =
                      newEntry(
                          extracted.getClazz(),
                          extracted.getMethodName(),
                          extracted.getParameters(),
                          extracted.getArgs(),
                          uniqueRequestId,
                          topic);
                  if (delayForAtLeast != null) {
                    entry.setNextAttemptTime(entry.getNextAttemptTime().plus(delayForAtLeast));
                  }
                  validator.validate(entry);
                  persistor.save(extracted.getTransaction(), entry);
                  extracted
                      .getTransaction()
                      .addPostCommitHook(
                          () -> {
                            listener.scheduled(entry);
                            if (entry.getTopic() != null) {
                              log.debug("Queued {} in topic {}", entry.description(), topic);
                            } else if (delayForAtLeast == null) {
                              submitNow(entry);
                              log.debug(
                                  "Scheduled {} for post-commit execution", entry.description());
                            } else if (delayForAtLeast.compareTo(attemptFrequency) < 0) {
                              scheduler.schedule(
                                  () -> submitNow(entry),
                                  delayForAtLeast.toMillis(),
                                  TimeUnit.MILLISECONDS);
                              log.info(
                                  "Scheduled {} for post-commit execution after at least {}",
                                  entry.description(),
                                  delayForAtLeast);
                            } else {
                              log.info(
                                  "Queued {} for execution after at least {}",
                                  entry.description(),
                                  delayForAtLeast);
                            }
                          });
                  return null;
                }));
  }

  private void submitNow(TransactionOutboxEntry entry) {
    submitter.submit(entry, this::processNow);
  }

  @Override
  @SuppressWarnings("WeakerAccess")
  public void processNow(TransactionOutboxEntry entry) {
    initialize();
    Boolean success = null;
    try {
      success =
          entry
              .getInvocation()
              .withinMDC(
                  () ->
                      transactionManager.inTransactionReturnsThrows(
                          tx -> {
                            if (!persistor.lock(tx, entry)) {
                              return false;
                            }
                            log.info("Processing {}", entry.description());
                            invoke(entry, tx);
                            if (entry.getUniqueRequestId() == null) {
                              persistor.delete(tx, entry);
                            } else {
                              log.debug(
                                  "Deferring deletion of {} by {}",
                                  entry.description(),
                                  retentionThreshold);
                              entry.setProcessed(true);
                              entry.setLastAttemptTime(Instant.now(clockProvider.get()));
                              entry.setNextAttemptTime(after(retentionThreshold));
                              persistor.update(tx, entry);
                            }
                            return true;
                          }));
    } catch (InvocationTargetException e) {
      updateAttemptCount(entry, e.getCause());
    } catch (Exception e) {
      updateAttemptCount(entry, e);
    }
    if (success != null) {
      if (success) {
        log.info("Processed {}", entry.description());
        listener.success(entry);
      } else {
        log.debug("Skipped task {} - may be locked or already processed", entry.getId());
      }
    }
  }

  @Override
  public void processBatchNow(List<TransactionOutboxEntry> entries) {
    if (entries == null || entries.isEmpty()) {
      return;
    }
    initialize();
    try {
      transactionManager.inTransactionThrows(
          tx -> {
            if (!persistor.lockBatch(tx, entries)) {
              log.debug("Could not lock all entries in batch, skipping processing.");
              return;
            }

            try {
              invokeBatchEntries(entries, tx);
              markExecutedBatchEntries(entries, tx);
              notifyListeners(entries);
            } catch (InvocationTargetException e) {
              handleBatchInvocationException(entries, tx, e.getCause());
            } catch (Exception e) {
              handleBatchInvocationException(entries, tx, e);
            }
          });
    } catch (Exception e) {
      log.warn("Failed to process batch", e);
    }
  }

  private void notifyListeners(List<TransactionOutboxEntry> entries) {
    for (TransactionOutboxEntry entry : entries) {
      listener.success(entry);
    }
  }

  private void markExecutedBatchEntries(List<TransactionOutboxEntry> entries, Transaction tx)
      throws Exception {
    List<TransactionOutboxEntry> entriesToUpdate = new ArrayList<>();
    List<TransactionOutboxEntry> entriesToDelete = new ArrayList<>();

    for (TransactionOutboxEntry entry : entries) {
      if (entry.getUniqueRequestId() == null) {
        entriesToDelete.add(entry);
      } else {
        log.debug("Deferring deletion of {} by {}", entry.description(), retentionThreshold);
        entry.setProcessed(true);
        entry.setLastAttemptTime(Instant.now(clockProvider.get()));
        entry.setNextAttemptTime(after(retentionThreshold));
        entriesToUpdate.add(entry);
      }
    }

    if (!entriesToDelete.isEmpty()) {
      persistor.deleteBatch(tx, entriesToDelete);
    }

    if (!entriesToUpdate.isEmpty()) {
      persistor.updateBatch(tx, entriesToUpdate);
    }
  }

  private void invokeBatchEntries(List<TransactionOutboxEntry> entries, Transaction tx)
      throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
    for (TransactionOutboxEntry entry : entries) {
      log.info("Processing item in batch: {}", entry.description());
      invoke(entry, tx);
      log.info("Processed item in batch: {}", entry.description());
    }
  }

  private void handleBatchInvocationException(
      List<TransactionOutboxEntry> entries, Transaction tx, Throwable e) {
    log.warn(
        "Failed to process batch, updating attempt count and notifying listeners. Error: {}",
        e.getMessage());
    try {
      updateAttemptCountForBatch(entries);
      persistor.updateBatch(tx, entries);
    } catch (Exception ex) {
      log.error(
          "Failed to update attempt count for batch. Entries may be retried more times than expected.",
          ex);
    }

    notifyListenersOfBatchFailure(entries, e);
    throw (RuntimeException) Utils.uncheckAndThrow(e); // to rollback the transaction
  }

  private void invoke(TransactionOutboxEntry entry, Transaction transaction)
      throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
    Object instance = instantiator.getInstance(entry.getInvocation().getClassName());
    log.debug("Created instance {}", instance);
    transactionManager
        .injectTransaction(entry.getInvocation(), transaction)
        .invoke(instance, listener);
  }

  private TransactionOutboxEntry newEntry(
      Class<?> clazz,
      String methodName,
      Class<?>[] params,
      Object[] args,
      String uniqueRequestId,
      String topic) {
    return TransactionOutboxEntry.builder()
        .id(UUID.randomUUID().toString())
        .invocation(
            new Invocation(
                instantiator.getName(clazz),
                methodName,
                params,
                args,
                serializeMdc && (MDC.getMDCAdapter() != null) ? MDC.getCopyOfContextMap() : null))
        .lastAttemptTime(null)
        .nextAttemptTime(clockProvider.get().instant())
        .uniqueRequestId(uniqueRequestId)
        .topic(topic)
        .build();
  }

  private void pushBack(Transaction transaction, TransactionOutboxEntry entry)
      throws OptimisticLockException {
    try {
      entry.setLastAttemptTime(clockProvider.get().instant());
      entry.setNextAttemptTime(after(attemptFrequency));
      validator.validate(entry);
      persistor.update(transaction, entry);
    } catch (OptimisticLockException e) {
      throw e;
    } catch (Exception e) {
      Utils.uncheckAndThrow(e);
    }
  }

  private Instant after(Duration duration) {
    return clockProvider.get().instant().plus(duration).truncatedTo(MILLIS);
  }

  private void updateAttemptCount(TransactionOutboxEntry entry, Throwable cause) {
    try {
      entry.setAttempts(entry.getAttempts() + 1);
      var blocked = (entry.getTopic() == null) && (entry.getAttempts() >= blockAfterAttempts);
      entry.setBlocked(blocked);
      transactionManager.inTransactionThrows(tx -> pushBack(tx, entry));
      listener.failure(entry, cause);
      if (blocked) {
        log.error(
            "Blocking failing entry {} after {} attempts: {}",
            entry.getId(),
            entry.getAttempts(),
            entry.description(),
            cause);
        listener.blocked(entry, cause);
      } else {
        logAtLevel(
            log,
            logLevelTemporaryFailure,
            "Temporarily failed to process entry {} : {}",
            entry.getId(),
            entry.description(),
            cause);
      }
    } catch (Exception e) {
      log.error(
          "Failed to update attempt count for {}. It may be retried more times than expected.",
          entry.description(),
          e);
    }
  }

  private void updateAttemptCountForBatch(List<TransactionOutboxEntry> entries) {
    for (TransactionOutboxEntry entry : entries) {
      entry.setAttempts(entry.getAttempts() + 1);
      entry.setBlocked(isEntryBlocked(entry));
    }
  }

  private void notifyListenersOfBatchFailure(
      List<TransactionOutboxEntry> entries, Throwable cause) {
    for (TransactionOutboxEntry entry : entries) {
      try {
        listener.failure(entry, cause);
        if (isEntryBlocked(entry)) {
          log.error(
              "Blocking failing entry {} after {} attempts: {}",
              entry.getId(),
              entry.getAttempts(),
              entry.description(),
              cause);
          listener.blocked(entry, cause);
        } else {
          logAtLevel(
              log,
              logLevelTemporaryFailure,
              "Temporarily failed to process entry {} : {}",
              entry.getId(),
              entry.description(),
              cause);
        }
      } catch (Exception e) {
        log.error("Failed to notify listener of failure for {}", entry.description(), e);
      }
    }
  }

  private boolean isEntryBlocked(TransactionOutboxEntry entry) {
    return entry.getTopic() == null && entry.getAttempts() >= blockAfterAttempts;
  }

  @ToString
  static class TransactionOutboxBuilderImpl extends TransactionOutboxBuilder {

    TransactionOutboxBuilderImpl() {
      super();
    }

    public TransactionOutboxImpl build() {
      Validator validator = new Validator(this.clockProvider);
      TransactionOutboxImpl impl =
          new TransactionOutboxImpl(
              transactionManager,
              persistor,
              Utils.firstNonNull(instantiator, Instantiator::usingReflection),
              Utils.firstNonNull(submitter, Submitter::withDefaultExecutor),
              Utils.firstNonNull(attemptFrequency, () -> Duration.of(2, MINUTES)),
              Utils.firstNonNull(logLevelTemporaryFailure, () -> Level.WARN),
              blockAfterAttempts < 1 ? 5 : blockAfterAttempts,
              flushBatchSize < 1 ? 4096 : flushBatchSize,
              clockProvider == null ? Clock::systemDefaultZone : clockProvider,
              Utils.firstNonNull(listener, () -> TransactionOutboxListener.EMPTY),
              serializeMdc == null || serializeMdc,
              validator,
              retentionThreshold == null ? Duration.ofDays(7) : retentionThreshold,
              useOrderedBatchProcessing != null && useOrderedBatchProcessing);
      validator.validate(impl);
      if (initializeImmediately == null || initializeImmediately) {
        impl.initialize();
      }
      return impl;
    }
  }

  @Accessors(fluent = true, chain = true)
  @Setter
  private class ParameterizedScheduleBuilderImpl implements ParameterizedScheduleBuilder {

    private String uniqueRequestId;
    private String ordered;
    private Duration delayForAtLeast;

    @Override
    public <T> T schedule(Class<T> clazz) {
      if (uniqueRequestId != null && uniqueRequestId.length() > 250) {
        throw new IllegalArgumentException("uniqueRequestId may be up to 250 characters");
      }
      return TransactionOutboxImpl.this.schedule(clazz, uniqueRequestId, ordered, delayForAtLeast);
    }
  }
}
