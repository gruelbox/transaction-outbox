package com.gruelbox.transactionoutbox;

import static com.gruelbox.transactionoutbox.spi.Utils.logAtLevel;
import static com.gruelbox.transactionoutbox.spi.Utils.uncheckedly;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.MINUTES;

import com.gruelbox.transactionoutbox.spi.ProxyFactory;
import com.gruelbox.transactionoutbox.spi.Utils;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import jdk.dynalink.linker.support.Lookup;
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

  private static final Method RUN_SERIALIZABLE_LAMBDA;

  static {
    try {
      RUN_SERIALIZABLE_LAMBDA = LambdaRunner.class.getDeclaredMethod("run", SerializableLambda.class);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

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
    return schedule(clazz, null);
  }

  @Override
  public TransactionOutboxEntry schedule(SerializableLambda runnable) {
    checkInitialized();
    checkThreadLocalTransactionManager();
    try {
      var txn = transactionManager.extractTransaction(RUN_SERIALIZABLE_LAMBDA, new SerializableLambda[] {runnable});
      return scheduleLambda(txn.getTransaction(), runnable);
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  private TransactionOutboxEntry schedule(Transaction transaction, SerializableLambda runnable) {
    checkInitialized();
    return scheduleLambda(transaction, runnable);
  }

  private TransactionOutboxEntry scheduleLambda(Transaction transaction, SerializableLambda runnable) {
    try {
      TransactionOutboxEntry entry =
          newEntry(
              LambdaRunner.class,
              "run",
              new Class<?>[] { SerializableLambda.class },
              new SerializableLambda[] {runnable},
              null,
              null,
              clockProvider.get().instant());
      persistor.save(transaction, entry);
      addPostExecutionHook(null, transaction, entry);
      return entry;
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  private void checkInitialized() {
    if (!initialized.get()) {
      throw new IllegalStateException("Not initialized");
    }
  }

  @Override
  public ParameterizedScheduleBuilder with() {
    return new ParameterizedScheduleBuilderImpl();
  }

  private boolean flushStale(Instant now) {
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
    return !batch.isEmpty();
  }

  @Override
  public boolean flush(Executor executor) {
    checkInitialized();
    Instant now = clockProvider.get().instant();
    List<CompletableFuture<Boolean>> futures = new ArrayList<>();

    futures.add(CompletableFuture.supplyAsync(() -> flushStale(now), executor));

    futures.add(
        CompletableFuture.runAsync(() -> expireIdempotencyProtection(now), executor)
            .thenApply(it -> false));

    var topics =
        transactionManager.inTransactionReturns(
            tx -> uncheckedly(() -> persistor.selectActiveTopics(tx)));
    if (!topics.isEmpty()) {
      topics.stream()
          .map(topic -> CompletableFuture.supplyAsync(() -> processNextInTopic(topic), executor))
          .forEach(futures::add);
    }

    var allResults = futures.stream().reduce((f1, f2) -> f1.thenCombine(f2, (d1, d2) -> d1 || d2));
    return allResults.map(CompletableFuture::join).orElse(false);
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
    checkInitialized();
    checkThreadLocalTransactionManager();
    log.info("Unblocking entry {} for retry.", entryId);
    try {
      return ((ThreadLocalContextTransactionManager) transactionManager)
          .requireTransactionReturns(tx -> persistor.unblock(tx, entryId));
    } catch (Exception e) {
      throw (RuntimeException) Utils.uncheckAndThrow(e);
    }
  }

  private void checkThreadLocalTransactionManager() {
    if (!(transactionManager instanceof ThreadLocalContextTransactionManager)) {
      throw new UnsupportedOperationException(
          "This method requires a ThreadLocalContextTransactionManager");
    }
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public boolean unblock(String entryId, Object transactionContext) {
    checkInitialized();
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

  private <T> T schedule(Class<T> clazz, ParameterizedScheduleBuilderImpl params) {
    checkInitialized();
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
                          params == null ? null : params.uniqueRequestId,
                          params == null ? null : params.ordered,
                          params == null
                              ? clockProvider.get().instant()
                              : clockProvider.get().instant().plus(params.delayForAtLeast));
                  Transaction txn = extracted.getTransaction();
                  persistor.save(txn, entry);
                  addPostExecutionHook(params, txn, entry);
                  return null;
                }));
  }

  private void addPostExecutionHook(
      ParameterizedScheduleBuilderImpl params, Transaction txn, TransactionOutboxEntry entry) {
    txn.addPostCommitHook(
        () -> {
          listener.scheduled(entry);
          if (entry.getTopic() != null) {
            log.debug("Queued {} in topic {}", entry.description(), entry.getTopic());
          } else if (params == null || params.delayForAtLeast == null) {
            submitNow(entry);
            log.debug("Scheduled {} for post-commit execution", entry.description());
          } else if (params.delayForAtLeast.compareTo(attemptFrequency) < 0) {
            scheduler.schedule(
                () -> submitNow(entry), params.delayForAtLeast.toMillis(), TimeUnit.MILLISECONDS);
            log.info(
                "Scheduled {} for post-commit execution after at least {}",
                entry.description(),
                params.delayForAtLeast);
          } else {
            log.info(
                "Queued {} for execution after at least {}",
                entry.description(),
                params.delayForAtLeast);
          }
        });
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
          transactionManager.inTransactionReturnsThrows(
              tx -> {
                if (!persistor.lock(tx, entry)) {
                  return false;
                }
                processWithExistingLock(tx, entry);
                return true;
              });
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

  private boolean processNextInTopic(String topic) {
    var attempted = new AtomicReference<TransactionOutboxEntry>();
    var success = false;
    try {
      success =
          transactionManager.inTransactionReturnsThrows(
              tx -> {
                var next = persistor.nextInTopic(tx, topic);
                if (next.isPresent()) {
                  if (next.get().getNextAttemptTime().isBefore(clockProvider.get().instant())) {
                    log.info("Topic {}: processing seq {}", topic, next.get().getSequence());
                    attempted.set(next.get());
                    processWithExistingLock(tx, next.get());
                    return true;
                  } else {
                    log.info(
                        "Topic {}: ignoring until {}",
                        topic,
                        ZonedDateTime.ofInstant(next.get().getNextAttemptTime(), ZoneId.of("UTC")));
                    return false;
                  }
                } else {
                  return false;
                }
              });
    } catch (InvocationTargetException e) {
      if (attempted.get() != null) {
        updateAttemptCount(attempted.get(), e.getCause());
      }
    } catch (Exception e) {
      if (attempted.get() != null) {
        updateAttemptCount(attempted.get(), e);
      }
    }
    if (success) {
      log.info("Processed {}", attempted.get().description());
      listener.success(attempted.get());
    }
    return success;
  }

  private void processWithExistingLock(Transaction tx, TransactionOutboxEntry entry)
      throws Exception {
    initialize();
    entry
        .getInvocation()
        .withinMDC(
            () -> {
              log.info("Processing {}", entry.description());
              invoke(entry, tx);
              if (entry.getUniqueRequestId() == null) {
                persistor.delete(tx, entry);
              } else {
                log.debug(
                    "Deferring deletion of {} by {}", entry.description(), retentionThreshold);
                entry.setProcessed(true);
                entry.setLastAttemptTime(Instant.now(clockProvider.get()));
                entry.setNextAttemptTime(after(retentionThreshold));
                persistor.update(tx, entry);
              }
              return true;
            });
  }

  private void invoke(TransactionOutboxEntry entry, Transaction transaction)
      throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
    Object instance =
        entry.getInvocation().getClassName().equals(LambdaRunner.class.getName())
            ? new LambdaRunner(new LambdaContext(instantiator))
            : instantiator.getInstance(entry.getInvocation().getClassName());
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
      String topic,
      Instant nextAttemptTime) {
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
            .lastAttemptTime(null)
            .nextAttemptTime(nextAttemptTime)
            .uniqueRequestId(uniqueRequestId)
            .topic(topic)
            .build();
    validator.validate(entry);
    return entry;
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
              retentionThreshold == null ? Duration.ofDays(7) : retentionThreshold);
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
      return TransactionOutboxImpl.this.schedule(clazz, this);
    }
  }
}
