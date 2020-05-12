package com.gruelbox.transactionoutbox;

import static com.gruelbox.transactionoutbox.Utils.uncheckedly;
import static java.time.temporal.ChronoUnit.MINUTES;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;
import javax.validation.ClockProvider;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.validator.internal.engine.DefaultClockProvider;
import org.slf4j.event.Level;

/**
 * An implementation of the <a
 * href="https://microservices.io/patterns/data/transactional-outbox.html">Transactional Outbox</a>
 * pattern for Java. See <a href="https://github.com/gruelbox/transaction-outbox">README</a> for
 * usage instructions.
 */
@Slf4j
public class TransactionOutbox {

  private static final int DEFAULT_FLUSH_BATCH_SIZE = 4096;

  @NotNull private final TransactionManager transactionManager;

  /**
   * The method {@link TransactionOutbox} uses to interact with the database. This encapsulates all
   * {@link TransactionOutbox} interaction with the database outside transaction management (which
   * is handled by the {@link TransactionManager}).
   *
   * <p>Defaults to a multi-platform SQL implementation that should not need to be changed in most
   * cases. If re-implementing this interface, read the documentation on {@link Persistor}
   * carefully.
   */
  @Valid @NotNull private final Persistor persistor;

  /**
   * Responsible for describing a class as a name and creating instances of that class at runtime
   * from the name. See {@link Instantiator} for more information.
   *
   * <p>Defaults to {@link Instantiator#usingReflection()}.
   */
  @Valid @NotNull private final Instantiator instantiator;

  /**
   * Used for scheduling background work.
   *
   * <p>If no submitter is specified, {@link TransactionOutbox} will use {@link
   * Submitter#withDefaultExecutor()}.
   *
   * <p>See {@link Submitter#withExecutor(Executor)} for more information on designing bespoke
   * submitters for remoting.
   */
  @NotNull private final Submitter submitter;

  /**
   * How often tasks should be re-attempted. This should be balanced with {@link #flushBatchSize}
   * and the frequency with which {@link #flush()} is called to achieve optimum throughput.
   *
   * <p>Defaults to 2 minutes.
   */
  @NotNull private final Duration attemptFrequency;

  /**
   * The log level to use when logging temporary task failures. Includes a full stack trace.
   * Defaults to {@code WARN} level, but you may wish to reduce it to a lower level if you consider
   * warnings to be incidents.
   */
  @NotNull private final Level logLevelTemporaryFailure;

  /** After now many attempts a task should be blacklisted. Defaults to 5. */
  @Min(1)
  private final int blacklistAfterAttempts;

  /**
   * How many items should be attempted in each flush. This should be balanced with {@link
   * #attemptFrequency} and the frequency with which {@link #flush()} is called to achieve optimum
   * throughput.
   *
   * <p>Defaults to 4096.
   */
  @Min(1)
  private final int flushBatchSize;

  /**
   * The {@link Clock} source. Generally best left alone except when testing. Defaults to the system
   * clock.
   */
  @NotNull private final ClockProvider clockProvider;

  /** Event listener for use in testing. */
  @NotNull private final TransactionOutboxListener listener;

  private final Validator validator;

  @Builder
  private TransactionOutbox(
      TransactionManager transactionManager,
      Instantiator instantiator,
      Submitter submitter,
      Duration attemptFrequency,
      int blacklistAfterAttempts,
      int flushBatchSize,
      ClockProvider clockProvider,
      TransactionOutboxListener listener,
      Persistor persistor,
      Level logLevelTemporaryFailure) {
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
    this.validator.validate(this);
    this.persistor.migrate(transactionManager);
  }

  /**
   * The main entry point for submitting new transaction outbox tasks.
   *
   * <p>Returns a proxy of {@code T} which, when called, will instantly return and schedule a call
   * of the <em>real</em> method to occur after the current transaction is committed (as such a
   * transaction needs to be active and accessible from {@link #transactionManager})
   *
   * <p>Usage:
   *
   * <pre>transactionOutbox.schedule(MyService.class)
   *   .runMyMethod("with", "some", "arguments");</pre>
   *
   * <p>This will write a record to the database using the supplied {@link Persistor} and {@link
   * Instantiator}, using the current database transaction, which will get rolled back if the rest
   * of the transaction is, and thus never processed. However, if the transaction is committed, the
   * real method will be called immediately afterwards using the supplied {@link #submitter}. Should
   * that fail, the call will be reattempted whenever {@link #flush()} is called, provided at least
   * {@link #attemptFrequency} has passed since the time the task was last attempted.
   *
   * @param clazz The class to proxy.
   * @param <T> The type to proxy.
   * @return The proxy of {@code T}.
   */
  public <T> T schedule(Class<T> clazz) {
    return Utils.createProxy(clazz, (method, args) -> uncheckedly(() -> schedule(method, args)));
  }

  /**
   * Identifies any stale tasks queued using {@link #schedule(Class)} (those which were queued more
   * than {@link #attemptFrequency} ago and have been tried less than {@link
   * #blacklistAfterAttempts} times) and attempts to resubmit them.
   *
   * <p>As long as {@link #submitter} is non-blocking (e.g. uses a bounded queue with a {@link
   * java.util.concurrent.RejectedExecutionHandler} which throws such as {@link
   * java.util.concurrent.ThreadPoolExecutor.AbortPolicy}), this method will return quickly.
   * However, if {@link #submitter} uses a bounded queue with a blocking policy, this method could
   * block for a long time, depending on how long the scheduled work takes and how large {@link
   * #flushBatchSize} is.
   *
   * <p>Calls {@link TransactionManager#inTransactionReturns(TransactionalSupplier)} to start a new
   * transaction for the fetch.
   *
   * @return true if any work was flushed.
   */
  @SuppressWarnings("UnusedReturnValue")
  public boolean flush() {
    log.info("Flushing stale tasks");
    var batch =
        transactionManager.inTransactionReturns(
            transaction -> {
              List<TransactionOutboxEntry> result = new ArrayList<>(flushBatchSize);
              uncheckedly(() -> persistor.selectBatch(transaction, flushBatchSize, clockProvider.getClock().instant()))
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
    log.info("Got batch of {}", batch.size());
    batch.forEach(this::submitNow);
    log.info("Submitted batch");
    return !batch.isEmpty();
  }

  /**
   * Marks a blacklisted entry back to not blacklisted and resets the attempt count. Requires an
   * active transaction.
   *
   * @param entryId The entry id.
   * @return True if the whitelisting request was successful. May return false if another thread
   *     whitelisted the entry first.
   */
  public boolean whitelist(String entryId) {
    log.info("Whitelisting entry {}", entryId);
    try {
      return transactionManager.requireTransactionReturns(tx -> persistor.whitelist(tx, entryId));
    } catch (Exception e) {
      throw (RuntimeException) Utils.uncheckAndThrow(e);
    }
  }

  private <T> T schedule(Method method, Object[] args) throws Exception {
    TransactionOutboxEntry entry = newEntry(method, args);
    validator.validate(entry);
    transactionManager.requireTransaction(
        transaction -> {
          persistor.save(transaction, entry);
          transaction.addPostCommitHook(() -> submitNow(entry));
        });
    log.debug("Scheduled {} for running after transaction commit", entry.description());
    return null;
  }

  private void submitNow(TransactionOutboxEntry entry) {
    submitter.submit(entry, this::processNow);
  }

  /**
   * Processes an entry immediately in the current thread. Intended for use in custom
   * implementations of {@link Submitter} and should not generally otherwise be called.
   *
   * @param entry The entry.
   */
  public void processNow(TransactionOutboxEntry entry) {
    try {
      var success =
          transactionManager.inTransactionReturnsThrows(
              transaction -> {
                if (!persistor.lock(transaction, entry)) {
                  return false;
                }
                log.info("Processing {}", entry.description());
                invoke(entry);
                persistor.delete(transaction, entry);
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

  private void invoke(TransactionOutboxEntry entry)
      throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
    Object instance = instantiator.getInstance(entry.getInvocation().getClassName());
    log.debug("Created instance {}", instance);
    Method method =
        instance
            .getClass()
            .getDeclaredMethod(
                entry.getInvocation().getMethodName(), entry.getInvocation().getParameterTypes());
    method.setAccessible(true);
    if (log.isDebugEnabled()) {
      log.debug(
          "Invoking method {} with args {}",
          method,
          Arrays.toString(entry.getInvocation().getArgs()));
    }
    method.invoke(instance, entry.getInvocation().getArgs());
  }

  private TransactionOutboxEntry newEntry(Method method, Object[] args) {
    return TransactionOutboxEntry.builder()
        .id(UUID.randomUUID().toString())
        .invocation(
            new Invocation(
                instantiator.getName(method.getDeclaringClass()),
                method.getName(),
                method.getParameterTypes(),
                args))
        .nextAttemptTime(clockProvider.getClock().instant().plus(attemptFrequency))
        .build();
  }

  private void pushBack(Transaction transaction, TransactionOutboxEntry entry)
      throws OptimisticLockException {
    try {
      entry.setNextAttemptTime(clockProvider.getClock().instant().plus(attemptFrequency));
      validator.validate(entry);
      persistor.update(transaction, entry);
    } catch (OptimisticLockException e) {
      throw e;
    } catch (Exception e) {
      Utils.uncheckAndThrow(e);
    }
  }

  private void updateAttemptCount(TransactionOutboxEntry entry, Throwable cause) {
    try {
      entry.setAttempts(entry.getAttempts() + 1);
      var blacklisted = entry.getAttempts() >= blacklistAfterAttempts;
      entry.setBlacklisted(blacklisted);
      entry.setNextAttemptTime(clockProvider.getClock().instant().plus(attemptFrequency));
      validator.validate(entry);
      transactionManager.inTransactionThrows(transaction -> persistor.update(transaction, entry));
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
  }

  private void logAtLevel(Level level, String message, Object... args) {
    switch (level) {
      case ERROR:
        log.error(message, args);
        break;
      case WARN:
        log.warn(message, args);
        break;
      case INFO:
        log.info(message, args);
        break;
      case DEBUG:
        log.debug(message, args);
        break;
      case TRACE:
        log.trace(message, args);
        break;
      default:
        log.warn(message, args);
        break;
    }
  }
}
