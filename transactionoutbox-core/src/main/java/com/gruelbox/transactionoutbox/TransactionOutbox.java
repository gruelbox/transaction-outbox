package com.gruelbox.transactionoutbox;

import static com.gruelbox.transactionoutbox.Utils.uncheckedly;
import static java.time.LocalDateTime.now;
import static java.time.temporal.ChronoUnit.MINUTES;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.validation.ClockProvider;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.validator.internal.engine.DefaultClockProvider;

/**
 * An implementation of the <a
 * href="https://microservices.io/patterns/data/transactional-outbox.html">Transactional Outbox</a>
 * pattern for Java. See <a href="https://github.com/gruelbox/transaction-outbox">README</a> for
 * usage instructions.
 */
@Slf4j
public class TransactionOutbox {

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
   * The executor used for scheduling background work.
   *
   * <p>Note that there are some important aspects that should be considered in the configuration of
   * this executor:
   *
   * <ul>
   *   <li>Should use a BOUNDED blocking queue implementation such as {@link ArrayBlockingQueue},
   *       otherwise under high volume, the queue may get so large it causes out-of-memory errors.
   *   <li>Should use a {@link java.util.concurrent.RejectedExecutionHandler} which either throws
   *       (such as {@link java.util.concurrent.ThreadPoolExecutor.AbortPolicy}), silently fails
   *       (such as {@link java.util.concurrent.ThreadPoolExecutor.DiscardPolicy}) or blocks the
   *       calling thread until a thread is available. It should <strong>not</strong> execute the
   *       work in the calling thread (e.g. {@link
   *       java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy}, since this could result in
   *       unpredictable effects with tasks assuming they will be run in a different thread context
   *       corrupting thread state. Generally, throwing or silently failing are preferred since this
   *       allows the database to absorb all backpressure, but if you have a strong reason to choose
   *       a blocking policy to enforce upstream backpressure, be aware that {@link #flush()} can
   *       potentially block for a long period of time too, so design any background processing
   *       which calls it accordingly (e.g. avoid calling from a timed scheduled job; perhaps
   *       instead simply loop it).
   *   <li>The queue can afford to be quite large in most realistic production deployments, and it
   *       is advised that it be so (10000+).
   * </ul>
   *
   * <p>If no executor service is specified, {@link TransactionOutbox} will use its own local {@link
   * ExecutorService}, sized to match {@link ForkJoinPool#commonPool()} (or one thread, whichever is
   * the larger), with a maximum queue size of 16384 before work is discarded.
   */
  @NotNull private final Executor executor;

  /**
   * How often tasks should be re-attempted. This should be balanced with {@link #flushBatchSize}
   * and the frequency with which {@link #flush()} is called to achieve optimum throughput.
   *
   * <p>Defaults to 2 minutes.
   */
  @NotNull private final Duration attemptFrequency;

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

  @Builder
  private TransactionOutbox(
      TransactionManager transactionManager,
      Instantiator instantiator,
      Executor executor,
      Duration attemptFrequency,
      int blacklistAfterAttempts,
      int flushBatchSize,
      ClockProvider clockProvider,
      TransactionOutboxListener listener,
      Persistor persistor) {
    this.transactionManager = transactionManager;
    this.instantiator = Utils.firstNonNull(instantiator, Instantiator::usingReflection);
    this.persistor = persistor;
    this.executor =
        Utils.firstNonNull(
            executor,
            () ->
                new ThreadPoolExecutor(
                    1,
                    Math.min(1, ForkJoinPool.commonPool().getParallelism()),
                    0L,
                    TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<Runnable>(16384)));
    this.attemptFrequency = Utils.firstNonNull(attemptFrequency, () -> Duration.of(2, MINUTES));
    this.blacklistAfterAttempts = blacklistAfterAttempts <= 1 ? 5 : blacklistAfterAttempts;
    this.flushBatchSize = flushBatchSize <= 1 ? 4096 : flushBatchSize;
    this.clockProvider = Utils.firstNonNull(clockProvider, () -> DefaultClockProvider.INSTANCE);
    this.listener = Utils.firstNonNull(listener, () -> entry -> {});
    Utils.validate(this);
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
   * real method will be called immediately afterwards using the supplied {@link #executor}. Should
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
   * <p>As long as {@link #executor} is non-blocking (i.e. uses a bounded queue with a {@link
   * java.util.concurrent.RejectedExecutionHandler} which throws such as {@link
   * java.util.concurrent.ThreadPoolExecutor.AbortPolicy}), this method will return quickly.
   * However, if {@link #executor} uses a bounded queue with a blocking policy, this method could
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
              uncheckedly(() -> persistor.selectBatch(transaction, flushBatchSize))
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
    batch.forEach(this::submitNow);
    return !batch.isEmpty();
  }

  private <T> T schedule(Method method, Object[] args) throws Exception {
    TransactionOutboxEntry entry = newEntry(method, args);
    transactionManager.requireTransaction(
        transaction -> {
          persistor.save(transaction, entry);
          transaction.addPostCommitHook(() -> submitNow(entry));
        });
    log.debug("Scheduled {} for running after transaction commit", entry.description());
    return null;
  }

  private void submitNow(TransactionOutboxEntry entry) {
    try {
      executor.execute(
          () -> {
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
              log.warn(
                  "Temporarily failed to process {}: {}",
                  entry.description(),
                  entry.description(),
                  e.getCause());
              updateAttemptCount(entry);
            } catch (Exception e) {
              log.warn(
                  "Temporarily failed to process {}: {}",
                  entry.description(),
                  entry.description(),
                  e);
              updateAttemptCount(entry);
            }
          });
      log.debug("Submitted {} for immediate processing", entry.description());
    } catch (RejectedExecutionException e) {
      log.debug("Queued {} for processing when executor is available", entry.description());
    } catch (Exception e) {
      log.warn(
          "Failed to submit {} for execution. It will be re-attempted later.",
          entry.description(),
          e);
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
        .nextAttemptTime(
            LocalDateTime.ofInstant(
                    clockProvider.getClock().instant(), clockProvider.getClock().getZone())
                .plus(attemptFrequency))
        .build();
  }

  private void pushBack(Transaction transaction, TransactionOutboxEntry entry)
      throws OptimisticLockException {
    try {
      entry.setNextAttemptTime(now().plus(attemptFrequency));
      persistor.update(transaction, entry);
    } catch (OptimisticLockException e) {
      throw e;
    } catch (Exception e) {
      Utils.uncheckAndThrow(e);
    }
  }

  private void updateAttemptCount(TransactionOutboxEntry entry) {
    try {
      entry.setAttempts(entry.getAttempts() + 1);
      entry.setBlacklisted(entry.getAttempts() >= blacklistAfterAttempts);
      entry.setNextAttemptTime(now().plus(attemptFrequency));
      transactionManager.inTransactionThrows(transaction -> persistor.update(transaction, entry));
    } catch (Exception e) {
      log.error(
          "Failed to update attempt count for {}. It may be retried more times than expected.",
          entry.description(),
          e);
    }
  }
}
