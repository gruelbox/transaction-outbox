package com.gruelbox.transactionoutbox;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Called by {@link TransactionOutbox} to submit work for background processing.
 */
public interface Submitter {

  /**
   * Schedules background work using a local {@link Executor} implementation. Note that the {@link
   * Runnable}s submitted to this will not be {@link java.io.Serializable} so will not be suitable
   * for remoting. Remote submission of work is not yet supported.
   *
   * <p>Note that there are some important aspects that should be considered in the configuration
   * of this executor:
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
   *       a blocking policy to enforce upstream backpressure, be aware that
   *       {@link TransactionOutbox#flush()} can potentially block for a long period of time too,
   *       so design any background processing which calls it accordingly (e.g. avoid calling from a
   *       timed scheduled job; perhaps instead simply loop it).
   *   <li>The queue can afford to be quite large in most realistic production deployments, and it
   *       is advised that it be so (10000+).
   * </ul>
   *
   * @return The submitter.
   */
  static Submitter withExecutor(Executor executor) {
    return new ExecutorSubmitter(executor);
  }

  /**
   * Schedules background worh with a {@link ThreadPoolExecutor}, sized to match {@link
   * ForkJoinPool#commonPool()} (or one thread, whichever is the larger), with a maximum queue size
   * of 16384 before work is discarded.
   *
   * @return The submitter.
   */
  static Submitter withDefaultExecutor() {
    return withExecutor(new ThreadPoolExecutor(
        1,
        Math.min(1, ForkJoinPool.commonPool().getParallelism()),
        0L,
        TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<Runnable>(16384)));
  }

  /**
   * Submits a transaction outbox task for processing. The {@link TransactionOutboxEntry} is
   * provided, along with a {@code localExecutor} which can run the work immediately. An
   * implementation may validly do any of the following:
   *
   * <ul>
   *   <li>Submit a call to {@code localExecutor} in a local thread, e.g. using an
   *   {@link Executor}. This is what implementations returned by
   *   {@link #withExecutor(Executor)} or {@link #withDefaultExecutor()} will do, and is
   *   recommended in almost all cases.</li>
   *   <li>Serialize the {@link TransactionOutboxEntry}, send it to another instance (e.g. via
   *   a queue) and have the handler code call
   *   {@link TransactionOutbox#processNow(TransactionOutboxEntry)}. Such an approach should
   *   not generally be necessary since {@link TransactionOutbox#flush()} is designed to be
   *   called repeatedly on multiple instances. This means there is a degree of load balancing
   *   built into the system, but when dealing with very high load, very low run-time tasks,
   *   this can get overwhelmed and direct multi-instance queuing can help balance the load
   *   at source. <strong>Note:</strong> it is recommended that the {@code invocation} property
   *   of the {@link TransactionOutboxEntry} be serialized using
   *   {@link InvocationSerializer#}</li>
   *   <li>Pass the {@code entry} directly to the {@code localExecutor}. This will run the
   *   work immediately in the calling thread and is therefore generally not recommended;
   *   the calling thread will be either the thread calling
   *   {@link TransactionOutbox#schedule(Class)} (effectively making the work synchronous)
   *   or the background poll thread (limiting work in progress to one). It can, however,
   *   be useful for test cases.</li>
   * </ul>
   *
   * @param entry The entry to process.
   * @param localExecutor Provides a means of running the work directly locally (it is
   *                      effectively just a call to
   *                      {@link TransactionOutbox#processNow(TransactionOutboxEntry)}).
   */
  void submit(TransactionOutboxEntry entry, Consumer<TransactionOutboxEntry> localExecutor);

}
