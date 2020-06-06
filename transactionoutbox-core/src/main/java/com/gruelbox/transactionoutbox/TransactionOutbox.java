package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.spi.BaseTransaction;
import com.gruelbox.transactionoutbox.spi.BaseTransactionManager;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import javax.validation.ClockProvider;
import lombok.ToString;
import org.slf4j.MDC;
import org.slf4j.event.Level;

/**
 * An implementation of the <a
 * href="https://microservices.io/patterns/data/transactional-outbox.html">Transactional Outbox</a>
 * pattern for Java. See <a href="https://github.com/gruelbox/transaction-outbox">README</a> for
 * usage instructions.
 */
public interface TransactionOutbox {

  /** @return A builder for creating a new instance of {@link TransactionOutbox}. */
  static TransactionOutboxBuilder builder() {
    return new TransactionOutboxBuilder();
  }

  /**
   * The main entry point for submitting new transaction outbox tasks.
   *
   * <p>Returns a proxy of {@code T} which, when called, will instantly return and schedule a call
   * of the <em>real</em> method to occur after the current transaction is committed (as such a
   * transaction needs to be active and accessible from {@link BaseTransactionManager}.)
   *
   * <p>Usage:
   *
   * <pre>transactionOutbox.schedule(MyService.class)
   *   .runMyMethod("with", "some", "arguments");</pre>
   *
   * <p>This will write a record to the database using the supplied {@link Persistor} and {@link
   * Instantiator}, using the current database transaction, which will get rolled back if the rest
   * of the transaction is, and thus never processed. However, if the transaction is committed, the
   * real method will be called immediately afterwards using the supplied {@link Submitter}. Should
   * that fail, the call will be reattempted whenever {@link #flush()} is called, provided at least
   * {@code attemptFrequency} has passed since the time the task was last attempted.
   *
   * @param clazz The class to proxy.
   * @param <T> The type to proxy.
   * @return The proxy of {@code X}.
   */
  <T> T schedule(Class<T> clazz);

  /**
   * Starts building a schedule request with parameterization. See {@link
   * ParameterizedScheduleBuilder#schedule(Class)} for more information.
   *
   * @return Builder.
   */
  ParameterizedScheduleBuilder with();

  /**
   * Identifies any stale tasks queued using {@link #schedule(Class)} (those which were queued more
   * than {@code attemptFrequency} ago and have been tried less than {@code blacklistAfterAttempts}
   * times) and attempts to resubmit them.
   *
   * <p>As long as the {@link Submitter} is non-blocking (e.g. uses a bounded queue with a {@link
   * java.util.concurrent.RejectedExecutionHandler} which throws such as {@link
   * java.util.concurrent.ThreadPoolExecutor.AbortPolicy}), this method will return quickly.
   * However, if the {@link Submitter}uses a bounded queue with a blocking policy, this method could
   * block for a long time, depending on how long the scheduled work takes and how large {@code
   * flushBatchSize} is.
   *
   * <p>Calls {@link BaseTransactionManager#transactionally(Function)} to start a new transaction
   * for the fetch.
   *
   * <p>Additionally, expires any records completed prior to the {@link
   * TransactionOutboxBuilder#retentionThreshold(Duration)}.
   *
   * @return true if any work was flushed.
   */
  boolean flush();

  /**
   * Identifies any stale tasks queued using {@link #schedule(Class)} (those which were queued more
   * than {@code attemptFrequency} ago and have been tried less than {@code blacklistAfterAttempts}
   * times) and attempts to resubmit them.
   *
   * <p>As long as the {@link Submitter} is non-blocking (e.g. uses a bounded queue with a {@link
   * java.util.concurrent.RejectedExecutionHandler} which throws such as {@link
   * java.util.concurrent.ThreadPoolExecutor.AbortPolicy}), this method will return quickly.
   * However, if the {@link Submitter}uses a bounded queue with a blocking policy, this method could
   * block for a long time, depending on how long the scheduled work takes and how large {@code
   * flushBatchSize} is.
   *
   * <p>Calls {@link BaseTransactionManager#transactionally(Function)} to start a new transaction
   * for the fetch.
   *
   * <p>Additionally, expires any records completed prior to the {@link
   * TransactionOutboxBuilder#retentionThreshold(Duration)}.
   *
   * @return true if any work was flushed.
   */
  CompletableFuture<Boolean> flushAsync();

  /**
   * Marks a blacklisted entry back to not blacklisted and resets the attempt count. Requires an
   * active transaction and a transaction manager that supports thread local context.
   *
   * @param entryId The entry id.
   * @return True if the whitelisting request was successful. May return false if another thread
   *     whitelisted the entry first.
   */
  boolean whitelist(String entryId);

  /**
   * Marks a blacklisted entry back to not blacklisted and resets the attempt count. Requires an
   * active transaction to be supplied.
   *
   * @param entryId The entry id.
   * @param transaction The transaction ({@link BaseTransactionManager} implementation specific).
   * @return True if the whitelisting request was successful. May return false if another thread
   *     whitelisted the entry first.
   */
  boolean whitelist(String entryId, BaseTransaction<?> transaction);

  /**
   * Marks a blacklisted entry back to not blacklisted and resets the attempt count. Requires an
   * active transaction and a transaction manager that supports thread local context. Will run
   * asynchronously if the underlying database API supports it.
   *
   * @param entryId The entry id.
   * @return True if the whitelisting request was successful. May return false if another thread
   *     whitelisted the entry first.
   */
  CompletableFuture<Boolean> whitelistAsync(String entryId);

  /**
   * Marks a blacklisted entry back to not blacklisted and resets the attempt count. Requires an
   * active transaction to be supplied.
   *
   * <p>Will run asynchronously if the underlying database API supports it.
   *
   * @param entryId The entry id.
   * @param transaction The transaction ({@link BaseTransactionManager} implementation specific).
   * @return True if the whitelisting request was successful. May return false if another thread
   *     whitelisted the entry first.
   */
  CompletableFuture<Boolean> whitelistAsync(String entryId, BaseTransaction<?> transaction);

  /**
   * Marks a blacklisted entry back to not blacklisted and resets the attempt count. Requires an
   * active transaction to be supplied via the implementation-specific context.
   *
   * <p>Will run asynchronously if the underlying database API supports it.
   *
   * @param entryId The entry id.
   * @param transactionContext The transaction context ({@link BaseTransactionManager}
   *     implementation specific).
   * @return True if the whitelisting request was successful. May return false if another thread
   *     whitelisted the entry first.
   */
  CompletableFuture<Boolean> whitelistAsync(String entryId, Object transactionContext);

  /**
   * Processes an entry immediately. Intended for use in custom implementations of {@link Submitter}
   * and should not generally otherwise be called.
   *
   * @param entry The entry.
   */
  @Beta
  CompletableFuture<Void> processNow(TransactionOutboxEntry entry);

  /** Builder for {@link TransactionOutbox}. */
  @ToString
  class TransactionOutboxBuilder {

    private BaseTransactionManager<?, ?> transactionManager;
    private Instantiator instantiator;
    private Submitter submitter;
    private Duration attemptFrequency;
    private int blacklistAfterAttempts;
    private int flushBatchSize;
    private ClockProvider clockProvider;
    private TransactionOutboxListener listener;
    private Persistor<?, ?> persistor;
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
    public TransactionOutboxBuilder transactionManager(
        BaseTransactionManager<?, ?> transactionManager) {
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
     * @param blacklistAfterAttempts After now many attempts a task should be blacklisted. Defaults
     *     to 5.
     * @return Builder.
     */
    public TransactionOutboxBuilder blacklistAfterAttempts(int blacklistAfterAttempts) {
      this.blacklistAfterAttempts = blacklistAfterAttempts;
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
     *     getting blacklisted.
     * @return Builder.
     */
    public TransactionOutboxBuilder listener(TransactionOutboxListener listener) {
      this.listener = listener;
      return this;
    }

    /**
     * @param persistor The method {@link TransactionOutbox} uses to interact with the database.
     *     This encapsulates all {@link TransactionOutbox} interaction with the database outside
     *     transaction management (which is handled by the {@link BaseTransactionManager}). Defaults
     *     to a multi-platform SQL implementation that should not need to be changed in most cases.
     *     If re-implementing this interface, read the documentation on {@link Persistor} carefully.
     * @return Builder.
     */
    public TransactionOutboxBuilder persistor(Persistor<?, ?> persistor) {
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
    @SuppressWarnings("unchecked")
    public TransactionOutbox build() {
      return new TransactionOutboxImpl(
          transactionManager,
          instantiator,
          submitter,
          attemptFrequency,
          blacklistAfterAttempts,
          flushBatchSize,
          clockProvider,
          listener,
          persistor,
          logLevelTemporaryFailure,
          serializeMdc,
          retentionThreshold);
    }
  }

  interface ParameterizedScheduleBuilder {

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
     *     maximum of 100 characters in length. It is advised that if these ids are client-supplied,
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
}
