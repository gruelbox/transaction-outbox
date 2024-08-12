package com.gruelbox.transactionoutbox;

import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;

/** Called by {@link TransactionOutbox} to submit work for background processing. */
public interface Submitter extends AutoCloseable {

  /**
   * Schedules background work using a local {@link Executor} implementation.
   *
   * <p>Shortcut for {@code ExecutorSubmitter.builder().executor(executor).build()}.
   *
   * @param executor The executor.
   * @return The submitter.
   */
  static Submitter withExecutor(Executor executor) {
    return ExecutorSubmitter.builder().executor(executor).build();
  }

  /**
   * Schedules background worh with a {@link ThreadPoolExecutor}, sized to match {@link
   * ForkJoinPool#commonPool()} (or one thread, whichever is the larger), with a maximum queue size
   * of 16384 before work is discarded.
   *
   * @return The submitter.
   */
  static Submitter withDefaultExecutor() {
    return ExecutorSubmitter.builder().build();
  }

  /**
   * Submits a transaction outbox task for processing. The {@link TransactionOutboxEntry} is
   * provided, along with a {@code localExecutor} which can run the work immediately. An
   * implementation may validly do any of the following:
   *
   * <ul>
   *   <li>Submit a call to {@code localExecutor} in a local thread, e.g. using an {@link Executor}.
   *       This is what implementations returned by {@link #withExecutor(Executor)} or {@link
   *       #withDefaultExecutor()} will do, and is recommended in almost all cases.
   *   <li>Serialize the {@link TransactionOutboxEntry}, send it to another instance (e.g. via a
   *       queue) and have the handler code call {@link
   *       TransactionOutbox#processNow(TransactionOutboxEntry)}. Such an approach should not
   *       generally be necessary since {@link TransactionOutbox#flush()} is designed to be called
   *       repeatedly on multiple instances. This means there is a degree of load balancing built
   *       into the system, but when dealing with very high load, very low run-time tasks, this can
   *       get overwhelmed and direct multi-instance queuing can help balance the load at source.
   *       <strong>Note:</strong> it is recommended that the {@code invocation} property of the
   *       {@link TransactionOutboxEntry} be serialized using {@link
   *       InvocationSerializer#createDefaultJsonSerializer()}
   *   <li>Pass the {@code entry} directly to the {@code localExecutor}. This will run the work
   *       immediately in the calling thread and is therefore generally not recommended; the calling
   *       thread will be either the thread calling {@link TransactionOutbox#schedule(Class)}
   *       (effectively making the work synchronous) or the background poll thread (limiting work in
   *       progress to one). It can, however, be useful for test cases.
   * </ul>
   *
   * @param entry The entry to process.
   * @param localExecutor Provides a means of running the work directly locally (it is effectively
   *     just a call to {@link TransactionOutbox#processNow(TransactionOutboxEntry)}).
   */
  void submit(TransactionOutboxEntry entry, Consumer<TransactionOutboxEntry> localExecutor);

  /**
   * Releases any releasable resource. The instance will become unusable after calling this method.
   */
  @Override
  default void close() {}
}
