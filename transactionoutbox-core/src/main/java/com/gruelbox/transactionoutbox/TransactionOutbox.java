package com.gruelbox.transactionoutbox;

import static com.gruelbox.transactionoutbox.Utils.logAtLevel;
import static com.gruelbox.transactionoutbox.Utils.uncheckedly;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.MINUTES;

import java.lang.reflect.InvocationTargetException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;
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
  @Valid @NotNull private final Persistor persistor;
  @Valid @NotNull private final Instantiator instantiator;
  @NotNull private final Submitter submitter;
  @NotNull private final Duration attemptFrequency;
  @NotNull private final Level logLevelTemporaryFailure;

  @Min(1)
  private final int blockedAfterAttempts;

  @Min(1)
  private final int flushBatchSize;

  @NotNull private final ClockProvider clockProvider;
  @NotNull private final TransactionOutboxListener listener;
  private final boolean serializeMdc;
  private final Validator validator;
  @NotNull private final Duration retentionThreshold;

  private TransactionOutbox(
      TransactionManager transactionManager,
      Instantiator instantiator,
      Submitter submitter,
      Duration attemptFrequency,
      int blockedAfterAttempts,
      int flushBatchSize,
      ClockProvider clockProvider,
      TransactionOutboxListener listener,
      Persistor persistor,
      Level logLevelTemporaryFailure,
      Boolean serializeMdc,
      Duration retentionThreshold) {
    this.transactionManager = transactionManager;
    this.instantiator = Utils.firstNonNull(instantiator, Instantiator::usingReflection);
    this.persistor = persistor;
    this.submitter = Utils.firstNonNull(submitter, Submitter::withDefaultExecutor);
    this.attemptFrequency = Utils.firstNonNull(attemptFrequency, () -> Duration.of(2, MINUTES));
    this.blockedAfterAttempts = blockedAfterAttempts < 1 ? 5 : blockedAfterAttempts;
    this.flushBatchSize = flushBatchSize < 1 ? DEFAULT_FLUSH_BATCH_SIZE : flushBatchSize;
    this.clockProvider = Utils.firstNonNull(clockProvider, () -> DefaultClockProvider.INSTANCE);
    this.listener = Utils.firstNonNull(listener, () -> new TransactionOutboxListener() {});
    this.logLevelTemporaryFailure = Utils.firstNonNull(logLevelTemporaryFailure, () -> Level.WARN);
    this.validator = new Validator(this.clockProvider);
    this.serializeMdc = serializeMdc == null || serializeMdc;
    this.retentionThreshold = retentionThreshold == null ? Duration.ofDays(7) : retentionThreshold;
    this.validator.validate(this);
    this.persistor.migrate(transactionManager);
  }

  /** @return A builder for creating a new instance of {@link TransactionOutbox}. */
  public static TransactionOutboxBuilder builder() {
    return new TransactionOutboxBuilder();
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
    return schedule(clazz, null);
  }

  /**
   * Starts building a schedule request with parameterization. See {@link
   * ParameterizedScheduleBuilder#schedule(Class)} for more information.
   *
   * @return Builder.
   */
  public ParameterizedScheduleBuilder with() {
    return new ParameterizedScheduleBuilderImpl();
  }

  /**
   * Identifies any stale tasks queued using {@link #schedule(Class)} (those which were queued more
   * than {@link #attemptFrequency} ago and have been tried less than {@link
   * #blockedAfterAttempts} times) and attempts to resubmit them.
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
   * <p>Additionally, expires any records completed prior to the {@link
   * TransactionOutboxBuilder#retentionThreshold(Duration)}.
   *
   * @return true if any work was flushed.
   */
  @SuppressWarnings("UnusedReturnValue")
  public boolean flush() {
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

  /**
   * Unblocks a blocked entry and resets the attempt count so that it will be
   * retried again. Requires an active transaction and a transaction manager that supports thread
   * local context.
   *
   * @param entryId The entry id.
   * @return True if the request to unblock the entry was successful. May return
   *     false if another thread unblocked the entry first.
   */
  public boolean unblock(String entryId) {
    if (!(transactionManager instanceof ThreadLocalContextTransactionManager)) {
      throw new UnsupportedOperationException(
          "This method requires a ThreadLocalContextTransactionManager");
    }
    log.info("Marking entry {} for retry.", entryId);
    try {
      return ((ThreadLocalContextTransactionManager) transactionManager)
          .requireTransactionReturns(tx -> persistor.unblock(tx, entryId));
    } catch (Exception e) {
      throw (RuntimeException) Utils.uncheckAndThrow(e);
    }
  }

  /**
   * Clears a failed entry of its failed state and resets the attempt count so that it will be
   * retried again. Requires an active transaction and a transaction manager that supports supplied
   * context.
   *
   * @param entryId The entry id.
   * @param transactionContext The transaction context ({@link TransactionManager} implementation
   *     specific).
   * @return True if the request to clear the failed state of the entry was successful. May return
   *     false if another thread cleared the entry of its failed state first.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public boolean unblock(String entryId, Object transactionContext) {
    if (!(transactionManager instanceof ParameterContextTransactionManager)) {
      throw new UnsupportedOperationException(
          "This method requires a ParameterContextTransactionManager");
    }
    log.info("Marking entry {} for retry", entryId);
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

  /**
   * Processes an entry immediately in the current thread. Intended for use in custom
   * implementations of {@link Submitter} and should not generally otherwise be called.
   *
   * @param entry The entry.
   */
  @SuppressWarnings("WeakerAccess")
  public void processNow(TransactionOutboxEntry entry) {
    try {
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
      var blocked = entry.getAttempts() >= blockedAfterAttempts;
      entry.setBlocked(blocked);
      entry.setNextAttemptTime(after(attemptFrequency));
      validator.validate(entry);
      transactionManager.inTransactionThrows(transaction -> persistor.update(transaction, entry));
      listener.failure(entry, cause);
      if (blocked) {
        log.error(
            "Marking entry {} as blocked after {} attempts: {}",
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

  /** Builder for {@link TransactionOutbox}. */
  @ToString
  public static class TransactionOutboxBuilder {

    private TransactionManager transactionManager;
    private Instantiator instantiator;
    private Submitter submitter;
    private Duration attemptFrequency;
    private int blockAfterAttempts;
    private int flushBatchSize;
    private ClockProvider clockProvider;
    private TransactionOutboxListener listener;
    private Persistor persistor;
    private Level logLevelTemporaryFailure;
    private Boolean serializeMdc;
    private Duration retentionThreshold;

    TransactionOutboxBuilder() {}

    /**
     * @param transactionManager Provides {@link TransactionOutbox} with the ability to start,
     *     commit and roll back transactions as well as interact with running transactions started
     *     outside.
     * @return Builder.
     */
    public TransactionOutboxBuilder transactionManager(TransactionManager transactionManager) {
      this.transactionManager = transactionManager;
      return this;
    }

    /**
     * @param instantiator Responsible for describing a class as a name and creating instances of
     *     that class at runtime from the name. See {@link Instantiator} for more information.
     *     Defaults to {@link Instantiator#usingReflection()}.
     * @return Builder.
     */
    public TransactionOutboxBuilder instantiator(Instantiator instantiator) {
      this.instantiator = instantiator;
      return this;
    }

    /**
     * @param submitter Used for scheduling background work. If no submitter is specified, {@link
     *     TransactionOutbox} will use {@link Submitter#withDefaultExecutor()}. See {@link
     *     Submitter#withExecutor(Executor)} for more information on designing bespoke submitters
     *     for remoting.
     * @return Builder.
     */
    public TransactionOutboxBuilder submitter(Submitter submitter) {
      this.submitter = submitter;
      return this;
    }

    /**
     * @param attemptFrequency How often tasks should be re-attempted. This should be balanced with
     *     {@link #flushBatchSize} and the frequency with which {@link #flush()} is called to
     *     achieve optimum throughput. Defaults to 2 minutes.
     * @return Builder.
     */
    public TransactionOutboxBuilder attemptFrequency(Duration attemptFrequency) {
      this.attemptFrequency = attemptFrequency;
      return this;
    }

    /**
     * @param blockAfterAttempts how many attempts a task should be retried before it is permanently blocked.
     * Defaults to 5.
     * @return Builder.
     */
    public TransactionOutboxBuilder blockAfterAttempts(int blockAfterAttempts) {
      this.blockAfterAttempts = blockAfterAttempts;
      return this;
    }

    /**
     * @param flushBatchSize How many items should be attempted in each flush. This should be
     *     balanced with {@link #attemptFrequency} and the frequency with which {@link #flush()} is
     *     called to achieve optimum throughput. Defaults to 4096.
     * @return Builder.
     */
    public TransactionOutboxBuilder flushBatchSize(int flushBatchSize) {
      this.flushBatchSize = flushBatchSize;
      return this;
    }

    /**
     * @param clockProvider The {@link Clock} source. Generally best left alone except when testing.
     *     Defaults to the system clock.
     * @return Builder.
     */
    public TransactionOutboxBuilder clockProvider(ClockProvider clockProvider) {
      this.clockProvider = clockProvider;
      return this;
    }

    /**
     * @param listener Event listener. Allows client code to react to tasks running, failing or
     *     getting blocked.
     * @return Builder.
     */
    public TransactionOutboxBuilder listener(TransactionOutboxListener listener) {
      this.listener = listener;
      return this;
    }

    /**
     * @param persistor The method {@link TransactionOutbox} uses to interact with the database.
     *     This encapsulates all {@link TransactionOutbox} interaction with the database outside
     *     transaction management (which is handled by the {@link TransactionManager}). Defaults to
     *     a multi-platform SQL implementation that should not need to be changed in most cases. If
     *     re-implementing this interface, read the documentation on {@link Persistor} carefully.
     * @return Builder.
     */
    public TransactionOutboxBuilder persistor(Persistor persistor) {
      this.persistor = persistor;
      return this;
    }

    /**
     * @param logLevelTemporaryFailure The log level to use when logging temporary task failures.
     *     Includes a full stack trace. Defaults to {@code WARN} level, but you may wish to reduce
     *     it to a lower level if you consider warnings to be incidents.
     * @return Builder.
     */
    public TransactionOutboxBuilder logLevelTemporaryFailure(Level logLevelTemporaryFailure) {
      this.logLevelTemporaryFailure = logLevelTemporaryFailure;
      return this;
    }

    /**
     * @param serializeMdc Determines whether to include any Slf4j {@link MDC} (Mapped Diagnostic
     *     Context) in serialized invocations and recreate the state in submitted tasks. Defaults to
     *     true.
     * @return Builder.
     */
    public TransactionOutboxBuilder serializeMdc(Boolean serializeMdc) {
      this.serializeMdc = serializeMdc;
      return this;
    }

    /**
     * @param retentionThreshold The length of time that any request with a unique client id will be
     *     remembered, such that if the same request is repeated within the threshold period, {@link
     *     AlreadyScheduledException} will be thrown.
     * @return Builder.
     */
    public TransactionOutboxBuilder retentionThreshold(Duration retentionThreshold) {
      this.retentionThreshold = retentionThreshold;
      return this;
    }

    /**
     * Creates and initialises the {@link TransactionOutbox}.
     *
     * @return The outbox implementation.
     */
    public TransactionOutbox build() {
      return new TransactionOutbox(
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
          retentionThreshold);
    }
  }

  public interface ParameterizedScheduleBuilder {

    /**
     * Specifies a unique id for the request. This defaults to {@code null}, but if non-null, will
     * cause the request to be retained in the database after completion for the specified {@link
     * TransactionOutboxBuilder#retentionThreshold(Duration)}, during which time any duplicate
     * requests to schedule the same request id will throw {@link AlreadyScheduledException}. This
     * allows tasks to be scheduled idempotently even if the request itself is not idempotent (e.g.
     * from a message queue listener, which can usually only work reliably on an "at least once"
     * basis).
     *
     * @param uniqueRequestId The unique request id. May be {@code null}, but if non-null may be a
     *     maximum of 250 characters in length. It is advised that if these ids are client-supplied,
     *     they be prepended with some sort of context identifier to ensure global uniqueness.
     * @return Builder.
     */
    ParameterizedScheduleBuilder uniqueRequestId(String uniqueRequestId);

    /**
     * Equivalent to {@link TransactionOutbox#schedule(Class)}, but applying additional parameters
     * to the request as configured using {@link TransactionOutbox#with()}.
     *
     * <p>Usage example:
     *
     * <pre>transactionOutbox.with()
     * .uniqueRequestId("my-request")
     * .schedule(MyService.class)
     * .runMyMethod("with", "some", "arguments");</pre>
     *
     * @param clazz The class to proxy.
     * @param <T> The type to proxy.
     * @return The proxy of {@code T}.
     */
    <T> T schedule(Class<T> clazz);
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
      return TransactionOutbox.this.schedule(clazz, uniqueRequestId);
    }
  }
}
