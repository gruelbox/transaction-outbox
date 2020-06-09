package com.gruelbox.transactionoutbox;

import static com.gruelbox.transactionoutbox.Utils.firstNonNull;
import static com.gruelbox.transactionoutbox.Utils.logAtLevel;
import static com.gruelbox.transactionoutbox.Utils.sneakyThrow;
import static com.gruelbox.transactionoutbox.Utils.uncheckedly;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

import com.gruelbox.transactionoutbox.spi.BaseTransaction;
import com.gruelbox.transactionoutbox.spi.BaseTransactionManager;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
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
class TransactionOutboxImpl<CN, TX extends BaseTransaction<CN>> implements TransactionOutbox {

  private static final int DEFAULT_FLUSH_BATCH_SIZE = 4096;

  @Valid @NotNull private final BaseTransactionManager<CN, TX> transactionManager;
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
  private final Method whitelistMethod;

  TransactionOutboxImpl(
      BaseTransactionManager<CN, TX> transactionManager,
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
    this.instantiator = firstNonNull(instantiator, Instantiator::usingReflection);
    this.persistor = persistor;
    this.submitter = firstNonNull(submitter, Submitter::withDefaultExecutor);
    this.attemptFrequency = firstNonNull(attemptFrequency, () -> Duration.of(2, MINUTES));
    this.blacklistAfterAttempts = blacklistAfterAttempts <= 1 ? 5 : blacklistAfterAttempts;
    this.flushBatchSize = flushBatchSize <= 1 ? DEFAULT_FLUSH_BATCH_SIZE : flushBatchSize;
    this.clockProvider = firstNonNull(clockProvider, () -> DefaultClockProvider.INSTANCE);
    this.listener = firstNonNull(listener, () -> new TransactionOutboxListener() {});
    this.logLevelTemporaryFailure = firstNonNull(logLevelTemporaryFailure, () -> Level.WARN);
    this.validator = new Validator(this.clockProvider);
    this.serializeMdc = serializeMdc == null ? true : serializeMdc;
    this.retentionThreshold = retentionThreshold == null ? Duration.ofDays(7) : retentionThreshold;
    this.validator.validate(this);
    this.whitelistMethod =
        uncheckedly(() -> getClass().getMethod("whitelistAsync", String.class, Object.class));
    publishInitializationEvents();
    this.persistor.migrate(transactionManager);
  }

  @Override
  public <X> X schedule(Class<X> clazz) {
    return schedule(clazz, null);
  }

  @SuppressWarnings("unchecked")
  private <T> T schedule(Class<T> clazz, String uniqueRequestId) {
    return Utils.createProxy(
        clazz,
        (method, args) ->
            uncheckedly(
                () -> {
                  TransactionalInvocation extracted =
                      transactionManager.extractTransaction(method, args);
                  var entry = newEntry(uniqueRequestId, extracted);
                  TX tx = (TX) extracted.getTransaction();
                  if (CompletableFuture.class.isAssignableFrom(method.getReturnType())) {
                    return submitAsFuture(tx, entry);
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
    Instant now = clockProvider.getClock().instant();
    return flushAsync(now)
        .thenApply(batch -> !batch.isEmpty())
        .thenCompose(
            didWorkFlushWork ->
                expireIdempotencyProtection(now)
                    .thenApply(didExpiryWork -> didWorkFlushWork || didExpiryWork));
  }

  private CompletableFuture<List<TransactionOutboxEntry>> flushAsync(Instant now) {
    log.info("Flushing stale tasks");
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
  public boolean whitelist(String entryId) {
    return Utils.join(whitelistAsync(entryId));
  }

  @Override
  public boolean whitelist(String entryId, BaseTransaction<?> transaction) {
    return Utils.join(whitelistAsync(entryId, transaction));
  }

  @Override
  public CompletableFuture<Boolean> whitelistAsync(String entryId) {
    TransactionalInvocation invocation =
        transactionManager.extractTransaction(whitelistMethod, new Object[] {entryId, null});
    return whitelistAsync(entryId, invocation.getTransaction());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> whitelistAsync(String entryId, BaseTransaction<?> tx) {
    log.info("Whitelisting entry {}", entryId);
    return persistor
        .whitelist((TX) tx, entryId)
        .thenApply(
            success -> {
              if (success) {
                log.info("Whitelisting of entry {} succeeded", entryId);
              } else {
                log.info("Whitelisting of entry {} failed", entryId);
              }
              return success;
            });
  }

  @Override
  public CompletableFuture<Boolean> whitelistAsync(String entryId, Object context) {
    if (context instanceof BaseTransaction) {
      return whitelistAsync(entryId, (BaseTransaction<?>) context);
    }
    TransactionalInvocation invocation =
        transactionManager.extractTransaction(whitelistMethod, new Object[] {entryId, context});
    return whitelistAsync(entryId, invocation.getTransaction());
  }

  @Override
  public CompletableFuture<Void> processNow(TransactionOutboxEntry entry) {
    return transactionManager
        .transactionally(transaction -> processNow(entry, transaction))
        .handle(
            (success, e) -> {
              if (e == null) {
                if (success) {
                  log.info("Processed({}) {}", entry.getAttempts() + 1, entry.description());
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
                        _1 -> {
                          if (entry.getUniqueRequestId() == null) {
                            return persistor.delete(tx, entry).thenApply(_2 -> true);
                          } else {
                            log.debug(
                                "Deferring deletion of {} by {}",
                                entry.description(),
                                retentionThreshold);
                            entry.setProcessed(true);
                            entry.setNextAttemptTime(after(retentionThreshold));
                            return persistor.update(tx, entry).thenApply(_2 -> true);
                          }
                        });
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
                long s = retentionThreshold.toSeconds();
                long days = s / 86400;
                long remaining = s % 86400;
                long hours = remaining / 3600;
                remaining = remaining % 3600;
                long minutes = remaining / 60;
                long seconds = remaining % 60;
                log.info(
                    "Expired idempotency protection on {} requests completed more than {} ago",
                    total,
                    String.format("%dd %02dh %02dm %02ds", days, hours, minutes, seconds));
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
              tx.addPostCommitHook(() -> submitNow(entry));
              log.debug("Scheduled {}", entry.description());
            });
  }

  private CompletableFuture<Void> submitNow(TransactionOutboxEntry entry) {
    submitter.submit(entry, this::processNow);
    return completedFuture(null);
  }

  private CompletableFuture<Void> invoke(TransactionOutboxEntry entry, TX transaction) {
    try {
      log.info("Processing({}) {}", entry.getAttempts() + 1, entry.description());
      Object instance = instantiator.getInstance(entry.getInvocation().getClassName());
      log.debug("Created instance {}", instance);
      Invocation invocation =
          transactionManager.injectTransaction(entry.getInvocation(), transaction);
      Object result = invocation.invoke(instance);
      log.debug("Successfully invoked, returned {}", result);
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
            .nextAttemptTime(after(attemptFrequency))
            .uniqueRequestId(uniqueRequestId)
            .build();
    validator.validate(entry);
    return entry;
  }

  private CompletableFuture<Void> pushBack(TX transaction, TransactionOutboxEntry entry) {
    entry.setNextAttemptTime(after(attemptFrequency));
    validator.validate(entry);
    return persistor.update(transaction, entry);
  }

  private Instant after(Duration duration) {
    return clockProvider.getClock().instant().plus(duration).truncatedTo(MILLIS);
  }

  private CompletableFuture<Void> updateAttemptCount(
      TransactionOutboxEntry entry, Throwable cause) {
    try {
      entry.setAttempts(entry.getAttempts() + 1);
      var blacklisted = entry.getAttempts() >= blacklistAfterAttempts;
      if (blacklisted) {
        log.error(
            "Blacklisting failing process after {} attempts: {}",
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
      entry.setBlacklisted(blacklisted);
      entry.setNextAttemptTime(after(attemptFrequency));
      validator.validate(entry);
      return transactionManager
          .transactionally(transaction -> persistor.update(transaction, entry))
          .thenRun(
              () -> {
                log.debug("Successfully updated {}", entry.description());
                onFailure(entry, cause);
                if (blacklisted) {
                  onBlacklisted(entry, cause);
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

  private void onBlacklisted(TransactionOutboxEntry entry, Throwable cause) {
    try {
      listener.blacklisted(entry, cause);
    } catch (Exception e1) {
      log.error("Error dispatching blacklisted event", e1);
    }
  }

  private void onFailure(TransactionOutboxEntry entry, Throwable cause) {
    try {
      listener.failure(entry, cause);
    } catch (Exception e1) {
      log.error("Error dispatching failure event", e1);
    }
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
