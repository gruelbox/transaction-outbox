package com.gruelbox.transactionoutbox;

import static com.ea.async.Async.await;
import static com.gruelbox.transactionoutbox.Utils.logAtLevel;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import javax.validation.ClockProvider;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.validator.constraints.Length;
import org.hibernate.validator.internal.engine.DefaultClockProvider;
import org.slf4j.MDC;
import org.slf4j.event.Level;

@Slf4j
class TransactionOutboxImpl<CN, CX, TX extends Transaction<CN, CX>> implements TransactionOutbox {

  private static final int DEFAULT_FLUSH_BATCH_SIZE = 4096;

  @Valid @NotNull private final TransactionManager<CN, CX, TX> transactionManager;
  @Valid @NotNull private final Persistor<CN, TX> persistor;
  @Valid @NotNull private final Instantiator instantiator;
  @NotNull private final Submitter submitter;
  @NotNull private final Duration attemptFrequency;
  @NotNull private final Level logLevelTemporaryFailure;

  @Min(1)
  private final int blacklistAfterAttempts;

  @Min(1)
  private final int flushBatchSize;

  @NotNull private final ClockProvider clockProvider;
  @NotNull private final TransactionOutboxListener listener;
  private final boolean serializeMdc;
  private final Validator validator;
  @NotNull private final Duration retentionThreshold;

  TransactionOutboxImpl(
      TransactionManager<CN, CX, TX> transactionManager,
      Instantiator instantiator,
      Submitter submitter,
      Duration attemptFrequency,
      int blacklistAfterAttempts,
      int flushBatchSize,
      ClockProvider clockProvider,
      TransactionOutboxListener listener,
      Persistor<CN, TX> persistor,
      Level logLevelTemporaryFailure,
      Boolean serializeMdc,
      Duration retentionThreshold) {
    this.transactionManager = transactionManager;
    this.instantiator = Utils.firstNonNull(instantiator, Instantiator::usingReflection);
    this.persistor = persistor;
    this.submitter = Utils.firstNonNull(submitter, Submitter::withDefaultExecutor);
    this.attemptFrequency = Utils.firstNonNull(attemptFrequency, () -> Duration.of(2, MINUTES));
    this.blacklistAfterAttempts = blacklistAfterAttempts <= 1 ? 5 : blacklistAfterAttempts;
    this.flushBatchSize = flushBatchSize <= 1 ? DEFAULT_FLUSH_BATCH_SIZE : flushBatchSize;
    this.clockProvider = Utils.firstNonNull(clockProvider, () -> DefaultClockProvider.INSTANCE);
    this.listener = Utils.firstNonNull(listener, () -> new TransactionOutboxListener() {});
    this.logLevelTemporaryFailure = Utils.firstNonNull(logLevelTemporaryFailure, () -> Level.WARN);
    this.validator = new Validator(this.clockProvider);
    this.serializeMdc = serializeMdc == null ? true : serializeMdc;
    this.retentionThreshold = retentionThreshold == null ? Duration.ofDays(7) : retentionThreshold;
    this.validator.validate(this);
    this.persistor.migrate(transactionManager);
  }

  @Override
  public <X> X schedule(Class<X> clazz) {
    return schedule(clazz, null);
  }

  @Override
  public ParameterizedScheduleBuilder with() {
    return new ParameterizedScheduleBuilderImpl();
  }

  @SuppressWarnings("UnusedReturnValue")
  @Override
  public CompletableFuture<Boolean> flush() {
    Instant now = clockProvider.getClock().instant();
    List<TransactionOutboxEntry> batch = await(flush(now));
    await(expireIdempotencyProtection(now));
    return completedFuture(!batch.isEmpty());
  }

  @Override
  public boolean whitelist(String entryId) {
    // TODO needs testing
    if (!(transactionManager instanceof ThreadLocalContextTransactionManager)) {
      throw new UnsupportedOperationException(
          "This method requires a ThreadLocalContextTransactionManager");
    }
    log.info("Whitelisting entry {}", entryId);
    try {
      return ((ThreadLocalContextTransactionManager<CN, CX, TX>) transactionManager)
          .requireTransactionReturns(tx -> persistor.whitelist(tx, entryId).get());
    } catch (Exception e) {
      throw (RuntimeException) Utils.uncheckAndThrow(e);
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes", "unused"})
  @Override
  public CompletableFuture<Boolean> whitelist(String entryId, Object transactionContext) {
    // TODO needs testing
    if (!(transactionManager instanceof ParameterContextTransactionManager)) {
      throw new UnsupportedOperationException(
          "This method requires a ParameterContextTransactionManager");
    }
    log.info("Whitelisting entry {}", entryId);
    TX tx;
    if (transactionContext instanceof Transaction) {
      tx = (TX) transactionContext;
    } else {
      tx =
          ((ParameterContextTransactionManager<CN, CX, TX>) transactionManager)
              .transactionFromContext((CX) transactionContext);
    }
    return persistor.whitelist(tx, entryId);
  }

  @Override
  public CompletableFuture<Void> processNow(TransactionOutboxEntry entry) {
    try {
      boolean success =
          await(transactionManager.transactionally(transaction -> processNow(entry, transaction)));
      if (success) {
        log.info("Processed {}", entry.description());
        listener.success(entry);
      } else {
        log.debug("Skipped task {} - may be locked or already processed", entry.getId());
      }
    } catch (Exception e) {
      await(updateAttemptCount(entry, e));
    }
    return completedFuture(null);
  }

  private CompletableFuture<List<TransactionOutboxEntry>> flush(Instant now) {
    log.info("Flushing stale tasks");
    List<TransactionOutboxEntry> batch =
        await(transactionManager.transactionally(tx -> selectBatch(tx, now)));
    log.debug("Got batch of {}", batch.size());
    batch.forEach(this::submitNow);
    log.debug("Submitted batch");
    return completedFuture(batch);
  }

  private CompletableFuture<List<TransactionOutboxEntry>> selectBatch(TX tx, Instant now) {
    List<TransactionOutboxEntry> result = new ArrayList<>(flushBatchSize);
    List<TransactionOutboxEntry> found = await(persistor.selectBatch(tx, flushBatchSize, now));
    found.forEach(
        entry -> {
          log.debug("Reprocessing {}", entry.description());
          try {
            await(pushBack(tx, entry));
            result.add(entry);
          } catch (OptimisticLockException e) {
            log.debug("Beaten to optimistic lock on {}", entry.description());
          }
        });
    return completedFuture(result);
  }

  private CompletableFuture<Void> expireIdempotencyProtection(Instant now) {
    long totalRecordsDeleted = 0;
    int recordsDeleted;
    do {
      recordsDeleted =
          await(transactionManager.transactionally(tx -> deleteProcessedAndExpired(tx, now)));
      totalRecordsDeleted += recordsDeleted;
    } while (recordsDeleted > 0);
    if (totalRecordsDeleted > 0) {
      long s = retentionThreshold.toSeconds();
      String duration = String.format("%dd:%02dh:%02dm", s / 3600, (s % 3600) / 60, (s % 60));
      log.info(
          "Expired idempotency protection on {} requests completed more than {} ago",
          totalRecordsDeleted,
          duration);
    } else {
      log.debug("No records found to delete as of {}", now);
    }
    return completedFuture(null);
  }

  private CompletableFuture<Integer> deleteProcessedAndExpired(TX tx, Instant now) {
    int result = await(persistor.deleteProcessedAndExpired(tx, flushBatchSize, now));
    return completedFuture(result);
  }

  private <T> T schedule(Class<T> clazz, String uniqueRequestId) {
    return Utils.createProxy(
        clazz,
        (method, args) ->
            Utils.uncheckedly(
                () -> {
                  TransactionalInvocation<TX> extracted =
                      transactionManager.extractTransaction(method, args);
                  var entry = newEntry(uniqueRequestId, extracted);
                  TX tx = extracted.getTransaction();
                  if (CompletableFuture.class.isAssignableFrom(method.getReturnType())) {
                    return submitAsFuture(tx, entry);
                  } else {
                    submitBlocking(tx, entry);
                    return null;
                  }
                }));
  }

  private void submitBlocking(TX tx, TransactionOutboxEntry entry) {
    try {
      submitAsFuture(tx, entry).get();
    } catch (ExecutionException e) {
      Utils.uncheckAndThrow(e.getCause());
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private CompletableFuture<Void> submitAsFuture(TX tx, TransactionOutboxEntry entry) {
    try {
      await(persistor.save(tx, entry));
      tx.addPostCommitHook(() -> submitNow(entry));
      log.debug("Scheduled {}", entry.description());
      return completedFuture(null);
    } catch (Exception e) {
      return failedFuture(e);
    }
  }

  private CompletableFuture<Void> submitNow(TransactionOutboxEntry entry) {
    submitter.submit(entry, this::processNow);
    return completedFuture(null);
  }

  private CompletableFuture<Boolean> processNow(TransactionOutboxEntry entry, TX tx) {
    boolean locked = await(persistor.lock(tx, entry));
    if (!locked) {
      return completedFuture(false);
    }
    log.info("Processing {}", entry.description());
    await(invoke(entry, tx));
    if (entry.getUniqueRequestId() == null) {
      await(persistor.delete(tx, entry));
    } else {
      log.debug("Deferring deletion of {} by {}", entry.description(), retentionThreshold);
      entry.setProcessed(true);
      entry.setNextAttemptTime(after(retentionThreshold));
      await(persistor.update(tx, entry));
    }
    return completedFuture(true);
  }

  private CompletableFuture<Void> invoke(TransactionOutboxEntry entry, TX transaction) {
    Object instance = instantiator.getInstance(entry.getInvocation().getClassName());
    log.debug("Created instance {}", instance);
    Invocation invocation =
        transactionManager.injectTransaction(entry.getInvocation(), transaction);
    try {
      Object result = invocation.invoke(instance);
      if (result instanceof CompletableFuture<?>) {
        return ((CompletableFuture<?>) result).thenApply(__ -> null);
      } else {
        return completedFuture(null);
      }
    } catch (InvocationTargetException e) {
      return failedFuture(e.getCause());
    } catch (Exception e) {
      return failedFuture(e);
    }
  }

  private TransactionOutboxEntry newEntry(
      String uniqueRequestId, TransactionalInvocation<TX> extracted) {
    return newEntry(
        extracted.getClazz(),
        extracted.getMethodName(),
        extracted.getParameters(),
        extracted.getArgs(),
        uniqueRequestId);
  }

  private TransactionOutboxEntry newEntry(
      Class<?> clazz, String methodName, Class<?>[] params, Object[] args, String uniqueRequestId) {
    var entry =
        TransactionOutboxEntry.builder()
            .id(UUID.randomUUID().toString())
            .invocation(
                new Invocation(
                    instantiator.getName(clazz),
                    methodName,
                    params,
                    args,
                    serializeMdc && (MDC.getMDCAdapter() != null)
                        ? MDC.getCopyOfContextMap()
                        : null))
            .nextAttemptTime(after(attemptFrequency))
            .uniqueRequestId(uniqueRequestId)
            .build();
    validator.validate(entry);
    return entry;
  }

  private CompletableFuture<Void> pushBack(TX transaction, TransactionOutboxEntry entry) {
    entry.setNextAttemptTime(after(attemptFrequency));
    validator.validate(entry);
    await(persistor.update(transaction, entry));
    return completedFuture(null);
  }

  private Instant after(Duration duration) {
    return clockProvider.getClock().instant().plus(duration).truncatedTo(MILLIS);
  }

  private CompletableFuture<Void> updateAttemptCount(
      TransactionOutboxEntry entry, Throwable cause) {
    try {
      entry.setAttempts(entry.getAttempts() + 1);
      var blacklisted = entry.getAttempts() >= blacklistAfterAttempts;
      entry.setBlacklisted(blacklisted);
      entry.setNextAttemptTime(after(attemptFrequency));
      validator.validate(entry);
      await(
          transactionManager.transactionally(transaction -> persistor.update(transaction, entry)));
      listener.failure(entry, cause);
      if (blacklisted) {
        log.error(
            "Blacklisting failing process after {} attempts: {}",
            entry.getAttempts(),
            entry.description(),
            cause);
        listener.blacklisted(entry, cause);
      } else {
        logAtLevel(
            log,
            logLevelTemporaryFailure,
            "Temporarily failed to process: {}",
            entry.description(),
            cause);
      }
    } catch (Exception e) {
      log.error(
          "Failed to update attempt count for {}. It may be retried more times than expected.",
          entry.description(),
          e);
    }
    return completedFuture(null);
  }

  private class ParameterizedScheduleBuilderImpl implements ParameterizedScheduleBuilder {

    @Length(max = 100)
    private String uniqueRequestId;

    @Override
    public ParameterizedScheduleBuilder uniqueRequestId(String uniqueRequestId) {
      this.uniqueRequestId = uniqueRequestId;
      return this;
    }

    @Override
    public <T> T schedule(Class<T> clazz) {
      validator.validate(this);
      return TransactionOutboxImpl.this.schedule(clazz, uniqueRequestId);
    }
  }
}
