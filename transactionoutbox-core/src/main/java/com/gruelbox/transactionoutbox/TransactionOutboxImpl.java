package com.gruelbox.transactionoutbox;

import static com.gruelbox.transactionoutbox.Utils.logAtLevel;
import static com.gruelbox.transactionoutbox.Utils.uncheckedly;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.MINUTES;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.validation.ClockProvider;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.validator.constraints.Length;
import org.hibernate.validator.internal.engine.DefaultClockProvider;
import org.slf4j.MDC;
import org.slf4j.event.Level;

@Slf4j
class TransactionOutboxImpl implements TransactionOutbox {

  private static final int DEFAULT_FLUSH_BATCH_SIZE = 4096;

  @NotNull private final TransactionManager transactionManager;
  @Valid @NotNull private final Persistor persistor;
  @Valid @NotNull private final Instantiator instantiator;
  @NotNull private final Submitter submitter;
  @NotNull private final Duration attemptFrequency;
  @NotNull private final Level logLevelTemporaryFailure;

  @Min(1)
  private final int blockAfterAttempts;

  @Min(1)
  private final int flushBatchSize;

  @NotNull private final ClockProvider clockProvider;
  @NotNull private final TransactionOutboxListener listener;
  private final boolean serializeMdc;
  private final Validator validator;
  @NotNull private final Duration retentionThreshold;
  private final AtomicBoolean initialized = new AtomicBoolean();

  private TransactionOutboxImpl(
      TransactionManager transactionManager,
      Instantiator instantiator,
      Submitter submitter,
      Duration attemptFrequency,
      int blockAfterAttempts,
      int flushBatchSize,
      ClockProvider clockProvider,
      TransactionOutboxListener listener,
      Persistor persistor,
      Level logLevelTemporaryFailure,
      Boolean serializeMdc,
      Duration retentionThreshold,
      Boolean initializeImmediately) {
    this.transactionManager = transactionManager;
    this.instantiator = Utils.firstNonNull(instantiator, Instantiator::usingReflection);
    this.persistor = persistor;
    this.submitter = Utils.firstNonNull(submitter, Submitter::withDefaultExecutor);
    this.attemptFrequency = Utils.firstNonNull(attemptFrequency, () -> Duration.of(2, MINUTES));
    this.blockAfterAttempts = blockAfterAttempts < 1 ? 5 : blockAfterAttempts;
    this.flushBatchSize = flushBatchSize < 1 ? DEFAULT_FLUSH_BATCH_SIZE : flushBatchSize;
    this.clockProvider = Utils.firstNonNull(clockProvider, () -> DefaultClockProvider.INSTANCE);
    this.listener = Utils.firstNonNull(listener, () -> new TransactionOutboxListener() {});
    this.logLevelTemporaryFailure = Utils.firstNonNull(logLevelTemporaryFailure, () -> Level.WARN);
    this.validator = new Validator(this.clockProvider);
    this.serializeMdc = serializeMdc == null || serializeMdc;
    this.retentionThreshold = retentionThreshold == null ? Duration.ofDays(7) : retentionThreshold;
    this.validator.validate(this);
    if (initializeImmediately == null || initializeImmediately) {
      initialize();
    }
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
    return schedule(clazz, null);
  }

  @Override
  public ParameterizedScheduleBuilder with() {
    return new ParameterizedScheduleBuilderImpl();
  }

  @SuppressWarnings("UnusedReturnValue")
  @Override
  public boolean flush() {
    if (!initialized.get()) {
      throw new IllegalStateException("Not initialized");
    }
    Instant now = clockProvider.getClock().instant();
    List<TransactionOutboxEntry> batch = flush(now);
    expireIdempotencyProtection(now);
    return !batch.isEmpty();
  }

  private List<TransactionOutboxEntry> flush(Instant now) {
    log.debug("Flushing stale tasks");
    var batch =
        transactionManager.inTransactionReturns(
            transaction -> {
              List<TransactionOutboxEntry> result = new ArrayList<>(flushBatchSize);
              uncheckedly(() -> persistor.selectBatch(transaction, flushBatchSize, now))
                  .forEach(
                      entry -> {
                        log.debug("Reprocessing {}", entry.description());
                        try {
                          pushBack(transaction, entry);
                          result.add(entry);
                        } catch (OptimisticLockException e) {
                          log.debug("Beaten to optimistic lock on {}", entry.description());
                        }
                      });
              return result;
            });
    log.debug("Got batch of {}", batch.size());
    batch.forEach(this::submitNow);
    log.debug("Submitted batch");
    return batch;
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
      long s = retentionThreshold.toSeconds();
      String duration = String.format("%dd:%02dh:%02dm", s / 3600, (s % 3600) / 60, (s % 60));
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

  private <T> T schedule(Class<T> clazz, String uniqueRequestId) {
    if (!initialized.get()) {
      throw new IllegalStateException("Not initialized");
    }
    return Utils.createProxy(
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
                          uniqueRequestId);
                  validator.validate(entry);
                  persistor.save(extracted.getTransaction(), entry);
                  extracted
                      .getTransaction()
                      .addPostCommitHook(
                          () -> {
                            listener.scheduled(entry);
                            submitNow(entry);
                          });
                  log.debug(
                      "Scheduled {} for running after transaction commit", entry.description());
                  return null;
                }));
  }

  private void submitNow(TransactionOutboxEntry entry) {
    submitter.submit(entry, this::processNow);
  }

  @Override
  @SuppressWarnings("WeakerAccess")
  public void processNow(TransactionOutboxEntry entry) {
    try {
      initialize();
      var success =
          transactionManager.inTransactionReturnsThrows(
              transaction -> {
                if (!persistor.lock(transaction, entry)) {
                  return false;
                }
                log.info("Processing {}", entry.description());
                invoke(entry, transaction);
                if (entry.getUniqueRequestId() == null) {
                  persistor.delete(transaction, entry);
                } else {
                  log.debug(
                      "Deferring deletion of {} by {}", entry.description(), retentionThreshold);
                  entry.setProcessed(true);
                  entry.setNextAttemptTime(after(retentionThreshold));
                  persistor.update(transaction, entry);
                }
                return true;
              });
      if (success) {
        log.info("Processed {}", entry.description());
        listener.success(entry);
      } else {
        log.debug("Skipped task {} - may be locked or already processed", entry.getId());
      }
    } catch (InvocationTargetException e) {
      updateAttemptCount(entry, e.getCause());
    } catch (Exception e) {
      updateAttemptCount(entry, e);
    }
  }

  private void invoke(TransactionOutboxEntry entry, Transaction transaction)
      throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
    Object instance = instantiator.getInstance(entry.getInvocation().getClassName());
    log.debug("Created instance {}", instance);
    transactionManager.injectTransaction(entry.getInvocation(), transaction).invoke(instance);
  }

  private TransactionOutboxEntry newEntry(
      Class<?> clazz, String methodName, Class<?>[] params, Object[] args, String uniqueRequestId) {
    return TransactionOutboxEntry.builder()
        .id(UUID.randomUUID().toString())
        .invocation(
            new Invocation(
                instantiator.getName(clazz),
                methodName,
                params,
                args,
                serializeMdc && (MDC.getMDCAdapter() != null) ? MDC.getCopyOfContextMap() : null))
        .nextAttemptTime(after(attemptFrequency))
        .uniqueRequestId(uniqueRequestId)
        .build();
  }

  private void pushBack(Transaction transaction, TransactionOutboxEntry entry)
      throws OptimisticLockException {
    try {
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
    return clockProvider.getClock().instant().plus(duration).truncatedTo(MILLIS);
  }

  private void updateAttemptCount(TransactionOutboxEntry entry, Throwable cause) {
    try {
      entry.setAttempts(entry.getAttempts() + 1);
      var blocked = entry.getAttempts() >= blockAfterAttempts;
      entry.setBlocked(blocked);
      entry.setNextAttemptTime(after(attemptFrequency));
      validator.validate(entry);
      transactionManager.inTransactionThrows(transaction -> persistor.update(transaction, entry));
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

  @ToString
  static class TransactionOutboxBuilderImpl extends TransactionOutboxBuilder {

    TransactionOutboxBuilderImpl() {
      super();
    }

    public TransactionOutboxImpl build() {
      return new TransactionOutboxImpl(
          transactionManager,
          instantiator,
          submitter,
          attemptFrequency,
          blockAfterAttempts,
          flushBatchSize,
          clockProvider,
          listener,
          persistor,
          logLevelTemporaryFailure,
          serializeMdc,
          retentionThreshold,
          initializeImmediately);
    }
  }

  private class ParameterizedScheduleBuilderImpl implements ParameterizedScheduleBuilder {

    @Length(max = 250)
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
