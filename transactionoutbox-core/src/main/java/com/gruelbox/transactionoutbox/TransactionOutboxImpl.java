package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.spi.BaseTransaction;
import com.gruelbox.transactionoutbox.spi.BaseTransactionManager;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.slf4j.event.Level;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.gruelbox.transactionoutbox.Utils.*;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

@Slf4j
class TransactionOutboxImpl<CN, TX extends BaseTransaction<CN>> implements TransactionOutbox, Validatable {

  private static final int DEFAULT_FLUSH_BATCH_SIZE = 4096;

  private final BaseTransactionManager<CN, TX> transactionManager;
  private final Persistor<CN, TX> persistor;
  private final Instantiator instantiator;
  private final Submitter submitter;
  private final Duration attemptFrequency;
  private final Level logLevelTemporaryFailure;
  private final Level logLevelProcessStartAndFinish;
  private final int blockAfterAttempts;
  private final int flushBatchSize;
  private final Supplier<Clock> clockProvider;
  private final TransactionOutboxListener listener;
  private final boolean serializeMdc;
  private final Validator validator;
  private final Duration retentionThreshold;
  private final AtomicBoolean initialized = new AtomicBoolean();
  private final ProxyFactory proxyFactory = new ProxyFactory();
  private final Method unblockMethod;

  TransactionOutboxImpl(
      BaseTransactionManager<CN, TX> transactionManager,
      Instantiator instantiator,
      Submitter submitter,
      Duration attemptFrequency,
      int blockAfterAttempts,
      int flushBatchSize,
      Supplier<Clock> clockProvider,
      TransactionOutboxListener listener,
      Persistor<CN, TX> persistor,
      Level logLevelTemporaryFailure,
      Level logLevelProcessStartAndFinish,
      Boolean serializeMdc,
      Duration retentionThreshold,
      Boolean initializeImmediately) {

    this.transactionManager = transactionManager;
    this.instantiator = firstNonNull(instantiator, Instantiator::usingReflection);
    this.persistor = persistor;
    this.submitter = firstNonNull(submitter, Submitter::withDefaultExecutor);
    this.attemptFrequency = firstNonNull(attemptFrequency, () -> Duration.of(2, MINUTES));
    this.blockAfterAttempts = blockAfterAttempts < 1 ? 5 : blockAfterAttempts;
    this.flushBatchSize = flushBatchSize < 1 ? DEFAULT_FLUSH_BATCH_SIZE : flushBatchSize;
    this.clockProvider = clockProvider == null ? Clock::systemDefaultZone : clockProvider;
    this.listener = firstNonNull(listener, () -> new TransactionOutboxListener() {});
    this.logLevelTemporaryFailure = firstNonNull(logLevelTemporaryFailure, () -> Level.WARN);
    this.logLevelProcessStartAndFinish =
        firstNonNull(logLevelProcessStartAndFinish, () -> Level.INFO);
    this.validator = new Validator(this.clockProvider);
    this.serializeMdc = serializeMdc == null || serializeMdc;
    this.retentionThreshold = retentionThreshold == null ? Duration.ofDays(7) : retentionThreshold;
    this.validator.validate(this);
    this.unblockMethod =
        uncheckedly(() -> getClass().getMethod("unblockAsync", String.class, Object.class));
    if (initializeImmediately == null || initializeImmediately) {
      initialize();
    }
  }

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

  @Override
  public void initialize() {
    if (initialized.compareAndSet(false, true)) {
      try {
        publishInitializationEvents();
        persistor.migrate(transactionManager);
      } catch (Exception e) {
        initialized.set(false);
        throw e;
      }
    }
  }

  @Override
  public <X> X schedule(Class<X> clazz) {
    return schedule(clazz, null);
  }

  @SuppressWarnings("unchecked")
  private <T> T schedule(Class<T> clazz, String uniqueRequestId) {
    if (!initialized.get()) {
      throw new IllegalStateException("Not initialized");
    }
    return proxyFactory.createProxy(
            clazz,
            (method, args) -> uncheckedly(
              () -> {
                TransactionalInvocation extracted =
                        transactionManager.extractTransaction(method, args);
                var entry = newEntry(uniqueRequestId, extracted);
                TX tx = (TX) extracted.getTransaction();
                if (CompletableFuture.class.isAssignableFrom(method.getReturnType())) {
                  return (T) submitAsFuture(tx, entry);
                } else {
                  submitBlocking(tx, entry);
                  return null;
                }
              }));
  }

  @Override
  public ParameterizedScheduleBuilder with() {
    return new ParameterizedScheduleBuilderImpl();
  }

  @Override
  public boolean flush() {
    return Utils.join(flushAsync());
  }

  @Override
  public CompletableFuture<Boolean> flushAsync() {
    Instant now = clockProvider.get().instant();
    return flushAsync(now)
        .thenApply(batch -> !batch.isEmpty())
        .thenCompose(
            didWorkFlushWork ->
                expireIdempotencyProtection(now)
                    .thenApply(didExpiryWork -> didWorkFlushWork || didExpiryWork));
  }

  private CompletableFuture<List<TransactionOutboxEntry>> flushAsync(Instant now) {
    if (!initialized.get()) {
      throw new IllegalStateException("Not initialized");
    }
    log.debug("Flushing stale tasks");
    return transactionManager
        .transactionally(tx -> selectBatch(tx, now))
        .thenApply(
            batch -> {
              log.debug("Got batch of {}", batch.size());
              batch.forEach(this::submitNow);
              log.debug("Submitted batch");
              return batch;
            });
  }

  @Override
  public boolean unblock(String entryId) {
    return Utils.join(unblockAsync(entryId));
  }

  @Override
  public boolean unblock(String entryId, BaseTransaction<?> transaction) {
    return Utils.join(unblockAsync(entryId, transaction));
  }

  @Override
  public CompletableFuture<Boolean> unblockAsync(String entryId) {
    TransactionalInvocation invocation =
        transactionManager.extractTransaction(unblockMethod, new Object[] {entryId, null});
    return unblockAsync(entryId, invocation.getTransaction());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> unblockAsync(String entryId, BaseTransaction<?> tx) {
    if (!initialized.get()) {
      throw new IllegalStateException("Not initialized");
    }
    log.info("Unblocking entry {}", entryId);
    return persistor
        .unblock((TX) tx, entryId)
        .thenApply(
            success -> {
              if (success) {
                log.info("Unblocking of entry {} succeeded", entryId);
              } else {
                log.info("Unblocking of entry {} failed", entryId);
              }
              return success;
            });
  }

  @Override
  public CompletableFuture<Boolean> unblockAsync(String entryId, Object context) {
    if (context instanceof BaseTransaction) {
      return unblockAsync(entryId, (BaseTransaction<?>) context);
    }
    TransactionalInvocation invocation =
        transactionManager.extractTransaction(unblockMethod, new Object[] {entryId, context});
    return unblockAsync(entryId, invocation.getTransaction());
  }

  @Override
  public CompletableFuture<Void> processNow(TransactionOutboxEntry entry) {
    return transactionManager
        .transactionally(transaction -> processNow(entry, transaction))
        .handle(
            (success, e) -> {
              if (e == null) {
                if (success) {
                  logAtLevel(
                      log,
                      logLevelProcessStartAndFinish,
                      "Processed({}) {}",
                      entry.getAttempts() + 1,
                      entry.description());
                  onSuccess(entry);
                } else {
                  log.debug("Skipped task {} - may be locked or already processed", entry.getId());
                }
                return CompletableFuture.<Void>completedFuture(null);
              } else {
                return updateAttemptCount(entry, e);
              }
            })
        .thenCompose(Function.identity());
  }

  private CompletableFuture<Boolean> processNow(TransactionOutboxEntry entry, TX tx) {
    return persistor
        .lock(tx, entry)
        .thenCompose(
            locked -> {
              if (!locked) {
                return completedFuture(false);
              } else {
                return invoke(entry, tx)
                    .thenCompose(
                        _1 -> entry.getInvocation().withinMdcUnchecked(() -> {
                          if (entry.getUniqueRequestId() == null) {
                            return persistor.delete(tx, entry).thenApply(_2 -> true);
                          } else {
                            log.debug(
                                    "Deferring deletion of {} by {}",
                                    entry.description(),
                                    retentionThreshold);
                            entry.setProcessed(true);
                            entry.setLastAttemptTime(Instant.now(clockProvider.get()));
                            entry.setNextAttemptTime(after(retentionThreshold));
                            return persistor.update(tx, entry).thenApply(_2 -> true);
                          }
                        }));
              }
            });
  }

  private CompletableFuture<List<TransactionOutboxEntry>> selectBatch(TX tx, Instant now) {
    TransactionOutboxEntry[] result = new TransactionOutboxEntry[flushBatchSize];
    AtomicInteger resultSize = new AtomicInteger();
    return persistor
        .selectBatch(tx, flushBatchSize, now)
        .thenCompose(
            found ->
                CompletableFuture.allOf(
                    found.stream()
                        .map(
                            entry ->
                                pushBack(tx, entry)
                                    .handle(
                                        (r, t) -> {
                                          try {
                                            if (t != null) throw Utils.sneakyThrow(t);
                                            log.debug("Pushed back {}", entry.description());
                                            result[resultSize.getAndIncrement()] = entry;
                                            return null;
                                          } catch (LockException e) {
                                            log.warn(
                                                "Beaten to lock on {} ({})",
                                                entry.description(),
                                                e.getMessage());
                                            return null;
                                          }
                                        }))
                        .toArray(CompletableFuture[]::new)))
        .thenApply(__ -> Arrays.asList(result).subList(0, resultSize.get()));
  }

  private CompletableFuture<Boolean> expireIdempotencyProtection(Instant now) {
    return transactionManager
        .transactionally(tx -> persistor.deleteProcessedAndExpired(tx, flushBatchSize, now))
        .thenApply(
            total -> {
              if (total > 0) {
                String duration =
                        String.format(
                                "%dd:%02dh:%02dm:%02ds",
                                retentionThreshold.toDaysPart(),
                                retentionThreshold.toHoursPart(),
                                retentionThreshold.toMinutesPart(),
                                retentionThreshold.toSecondsPart());
                log.info(
                        "Expired idempotency protection on {} requests completed more than {} ago",
                        total,
                        duration);
                return true;
              } else {
                log.debug("No records found to delete as of {}", now);
                return false;
              }
            });
  }

  private void submitBlocking(TX tx, TransactionOutboxEntry entry) {
    Utils.join(submitAsFuture(tx, entry));
  }

  private CompletableFuture<Void> submitAsFuture(TX tx, TransactionOutboxEntry entry) {
    return persistor
        .save(tx, entry)
        .thenRun(
            () -> {
              tx.addPostCommitHook(
                  () -> {
                    listener.scheduled(entry);
                    return submitNow(entry);
                  });
              log.debug("Scheduled {}", entry.description());
            });
  }

  private CompletableFuture<Void> submitNow(TransactionOutboxEntry entry) {
    submitter.submit(entry, this::processNow);
    return completedFuture(null);
  }

  private CompletableFuture<Void> invoke(TransactionOutboxEntry entry, TX transaction) {
    try {
      return entry.getInvocation().withinMDC(() -> {
        logAtLevel(
                log,
                logLevelProcessStartAndFinish,
                "Processing({}) {}",
                entry.getAttempts() + 1,
                entry.description());
        Object instance = instantiator.getInstance(entry.getInvocation().getClassName());
        log.debug("Created instance {}", instance);
        Invocation invocation =
                transactionManager.injectTransaction(entry.getInvocation(), transaction);
        Object result = invocation.invoke(instance, listener);
        log.debug("Successfully invoked, returned {}", result);
        if (result instanceof CompletableFuture<?>) {
          return ((CompletableFuture<?>) result).thenApply(__ -> null);
        } else {
          return completedFuture(null);
        }
      });
    } catch (InvocationTargetException e) {
      return failedFuture(e.getCause());
    } catch (Exception e) {
      return failedFuture(e);
    }
  }

  private TransactionOutboxEntry newEntry(
      String uniqueRequestId, TransactionalInvocation extracted) {
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
            .lastAttemptTime(null)
            .nextAttemptTime(after(attemptFrequency))
            .uniqueRequestId(uniqueRequestId)
            .build();
    validator.validate(entry);
    return entry;
  }

  private CompletableFuture<Void> pushBack(TX transaction, TransactionOutboxEntry entry) {
    entry.setLastAttemptTime(clockProvider.get().instant());
    entry.setNextAttemptTime(after(attemptFrequency));
    validator.validate(entry);
    return persistor.update(transaction, entry);
  }

  private Instant after(Duration duration) {
    return clockProvider.get().instant().plus(duration).truncatedTo(MILLIS);
  }

  private CompletableFuture<Void> updateAttemptCount(
      TransactionOutboxEntry entry, Throwable cause) {
    try {
      entry.setAttempts(entry.getAttempts() + 1);
      var blocked = entry.getAttempts() >= blockAfterAttempts;
      if (blocked) {
        log.error(
            "Blocking failing process after {} attempts: {}",
            entry.getAttempts(),
            entry.description(),
            cause);
      } else {
        if (!logAtLevel(
            log,
            logLevelTemporaryFailure,
            "Failed({}): {}",
            entry.getAttempts(),
            entry.description(),
            cause)) {
          log.info(
              "Failed({}): {} ({} - {})",
              entry.getAttempts(),
              entry.description(),
              cause.getClass().getSimpleName(),
              cause.getMessage());
        }
      }
      entry.setBlocked(blocked);
      entry.setNextAttemptTime(after(attemptFrequency));
      validator.validate(entry);
      return transactionManager
          .transactionally(transaction -> persistor.update(transaction, entry))
          .thenRun(
              () -> {
                log.debug("Successfully updated {}", entry.description());
                onFailure(entry, cause);
                if (blocked) {
                  onBlocked(entry, cause);
                }
              })
          .exceptionally(
              t -> {
                try {
                  throw sneakyThrow(t);
                } catch (LockException e) {
                  log.warn(
                      "Failed to update attempt count for {} due to crossed lock. It may be "
                          + "retried more times than expected. This warning is expected at high "
                          + "volumes on databases that do not support SKIP LOCKED or equivalent "
                          + "features and can be ignored."
                          + e.getMessage(),
                      entry.description());
                } catch (Exception e) {
                  log.error(
                      "Failed to update attempt count for {} due to unexpected error. It may be "
                          + "retried more times than expected.",
                      entry.description(),
                      e);
                }
                return null;
              });
    } catch (Exception e) {
      log.error(
          "Failed to update attempt count for {}. It may be retried more times than expected.",
          entry.description(),
          e);
      return completedFuture(null);
    }
  }

  private void publishInitializationEvents() {
    InitializationEventBusImpl eventBus = new InitializationEventBusImpl();
    eventBus.subscribeAll(this);
    eventBus.publishAll(this);
  }

  private void onSuccess(TransactionOutboxEntry entry) {
    try {
      listener.success(entry);
    } catch (Exception e1) {
      log.error("Error dispatching success event", e1);
    }
  }

  private void onBlocked(TransactionOutboxEntry entry, Throwable cause) {
    try {
      listener.blocked(entry, cause);
    } catch (Exception e1) {
      log.error("Error dispatching blocked event", e1);
    }
  }

  private void onFailure(TransactionOutboxEntry entry, Throwable cause) {
    try {
      listener.failure(entry, cause);
    } catch (Exception e1) {
      log.error("Error dispatching failure event", e1);
    }
  }

  @ToString
  static class Builder extends TransactionOutboxBuilder {

    Builder() {
      super();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public TransactionOutbox build() {
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
          logLevelProcessStartAndFinish,
          serializeMdc,
          retentionThreshold,
          initializeImmediately);
    }
  }

  private class ParameterizedScheduleBuilderImpl implements ParameterizedScheduleBuilder {

    private String uniqueRequestId;

    @Override
    public ParameterizedScheduleBuilder uniqueRequestId(String uniqueRequestId) {
      this.uniqueRequestId = uniqueRequestId;
      return this;
    }

    @Override
    public <T> T schedule(Class<T> clazz) {
      if (uniqueRequestId != null && uniqueRequestId.length() > 250) {
        throw new IllegalArgumentException("uniqueRequestId may be up to 250 characters");
      }
      return TransactionOutboxImpl.this.schedule(clazz, uniqueRequestId);
    }
  }
}
