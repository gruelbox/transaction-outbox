package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.spi.Utils;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.event.Level;

/**
 * Schedules background work using a local {@link Executor} implementation. Note that the {@link
 * Runnable}s submitted to this will not be {@link java.io.Serializable} so will not be suitable for
 * remoting. Remote submission of work is not yet supported.
 *
 * <p>Note that there are some important aspects that should be considered in the configuration of
 * this executor:
 *
 * <ul>
 *   <li>Should use a BOUNDED blocking queue implementation such as {@link ArrayBlockingQueue},
 *       otherwise under high volume, the queue may get so large it causes out-of-memory errors.
 *   <li>Should use a {@link java.util.concurrent.RejectedExecutionHandler} which either throws
 *       (such as {@link java.util.concurrent.ThreadPoolExecutor.AbortPolicy}), silently fails (such
 *       as {@link java.util.concurrent.ThreadPoolExecutor.DiscardPolicy}) or blocks the calling
 *       thread until a thread is available. It should <strong>not</strong> execute the work in the
 *       calling thread (e.g. {@link java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy},
 *       since this could result in unpredictable effects with tasks assuming they will be run in a
 *       different thread context corrupting thread state. Generally, throwing or silently failing
 *       are preferred since this allows the database to absorb all backpressure, but if you have a
 *       strong reason to choose a blocking policy to enforce upstream backpressure, be aware that
 *       {@link TransactionOutbox#flush()} can potentially block for a long period of time too, so
 *       design any background processing which calls it accordingly (e.g. avoid calling from a
 *       timed scheduled job; perhaps instead simply loop it).
 *   <li>The queue can afford to be quite large in most realistic production deployments, and it is
 *       advised that it be so (10000+).
 * </ul>
 */
@Slf4j
public class ExecutorSubmitter implements Submitter, Validatable {

  @SuppressWarnings("JavaDoc")
  private final Executor executor;

  private final boolean shutdownExecutorOnClose;

  @SuppressWarnings("JavaDoc")
  private final Level logLevelWorkQueueSaturation;

  ExecutorSubmitter(Executor executor, Level logLevelWorkQueueSaturation) {
    if (executor != null) {
      this.executor = executor;
      shutdownExecutorOnClose = false;
    } else {
      // JDK bug means this warning can't be fixed
      //noinspection Convert2Diamond
      this.executor =
          new ThreadPoolExecutor(
              1,
              Math.max(1, ForkJoinPool.commonPool().getParallelism()),
              0L,
              TimeUnit.MILLISECONDS,
              new ArrayBlockingQueue<>(16384));
      shutdownExecutorOnClose = true;
    }
    this.logLevelWorkQueueSaturation = logLevelWorkQueueSaturation;
  }

  public static ExecutorSubmitterBuilder builder() {
    return new ExecutorSubmitterBuilder();
  }

  @Override
  public void submit(TransactionOutboxEntry entry, Consumer<TransactionOutboxEntry> localExecutor) {
    try {
      executor.execute(() -> localExecutor.accept(entry));
      log.debug("Submitted {} for immediate processing", entry.description());
    } catch (RejectedExecutionException e) {
      Utils.logAtLevel(
          log,
          logLevelWorkQueueSaturation,
          "Queued {} for processing when executor is available",
          entry.description());
    } catch (Exception e) {
      log.warn(
          "Failed to submit {} for execution. It will be re-attempted later.",
          entry.description(),
          e);
    }
  }

  @Override
  public void validate(Validator validator) {
    validator.notNull("executor", executor);
    validator.notNull("logLevelWorkQueueSaturation", logLevelWorkQueueSaturation);
  }

  @Override
  public void close() {
    if (!shutdownExecutorOnClose) {
      return;
    }
    if (!(executor instanceof ExecutorService)) {
      return;
    }
    Utils.shutdown((ExecutorService) executor);
  }

  public static class ExecutorSubmitterBuilder {
    private Executor executor;
    private Level logLevelWorkQueueSaturation;
    private boolean logLevelWorkQueueSaturationSet;

    ExecutorSubmitterBuilder() {}

    /**
     * @param executor The executor to use. If not provided, a {@link ThreadPoolExecutor}, sized to
     *     match {@link * ForkJoinPool#commonPool()} (or one thread, whichever is the larger), with
     *     a maximum queue size * of 16384 will be used.
     */
    public ExecutorSubmitterBuilder executor(Executor executor) {
      this.executor = executor;
      return this;
    }

    /**
     * @param logLevelWorkQueueSaturation The log level to use when work submission hits the
     *     executor queue limit. This usually indicates saturation and may be of greater interest
     *     than the default {@code DEBUG} level.
     */
    public ExecutorSubmitterBuilder logLevelWorkQueueSaturation(Level logLevelWorkQueueSaturation) {
      this.logLevelWorkQueueSaturation = logLevelWorkQueueSaturation;
      logLevelWorkQueueSaturationSet = true;
      return this;
    }

    public ExecutorSubmitter build() {
      Level logLevelWorkQueueSaturationToUse = logLevelWorkQueueSaturation;
      if (!logLevelWorkQueueSaturationSet) {
        logLevelWorkQueueSaturationToUse = Level.DEBUG;
      }
      return new ExecutorSubmitter(this.executor, logLevelWorkQueueSaturationToUse);
    }

    public String toString() {
      return "ExecutorSubmitter.ExecutorSubmitterBuilder(executor="
          + this.executor
          + ", logLevelWorkQueueSaturation$value="
          + logLevelWorkQueueSaturation
          + ")";
    }
  }
}
