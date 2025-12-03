package com.gruelbox.transactionoutbox;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

/** A listener for events fired by {@link TransactionOutbox}. */
public interface TransactionOutboxListener {

  TransactionOutboxListener EMPTY = new TransactionOutboxListener() {};

  /**
   * Fired when a transaction outbox task is scheduled.
   *
   * <p>This event is not guaranteed to fire in the event of a JVM failure or power loss. It is
   * fired <em>after</em> the commit to the database adding the scheduled task but before the task
   * is submitted for processing. It will, except in extreme circumstances (although this is not
   * guaranteed), fire prior to any subsequent {@link #success(TransactionOutboxEntry)} or {@link
   * #failure(TransactionOutboxEntry, Throwable)}.
   *
   * @param entry The outbox entry scheduled.
   */
  default void scheduled(TransactionOutboxEntry entry) {
    // No-op
  }

  /**
   * Implement this method to intercept and decorate all outbox invocations. In general, you should
   * call {@code invocation.run()} which actually calls the underlying method, unless you are
   * deliberately trying to suppress the method call.
   *
   * <p>This method is called immediately before your method is invoked. A fair bit of work has
   * already been done by this point (MDC initialised, transaction started, database record locked
   * etc) so it's a good place to do database-related things, but a poor place to initialise things
   * like session state (such as OTEL traces). For that, use {@link
   * #wrapInvocationAndInit(Invocator)}.
   *
   * @param invocator A runnable which performs the work of the scheduled task.
   * @throws IllegalAccessException If thrown by the method invocation.
   * @throws IllegalArgumentException If thrown by the method invocation.
   * @throws InvocationTargetException If thrown by the method invocation.
   */
  default void wrapInvocation(Invocator invocator)
      throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    invocator.run();
  }

  /**
   * Wraps an entire invocation, including the work do obtain a database lock. This is a good place
   * to initialise session state from {@link Invocation#getSession()} (using {@link
   * Invocator#getInvocation()}) since then all subsequent database activity is performed within
   * that session.
   *
   * <p>NOTE that there is no guarantee that your method will actually be invoked at this point.
   * There is no active database transaction, the database record hasn't been locked and important
   * checks haven't been performed. This is intended purely for state management.
   *
   * @param invocator A runnable which performs the work of the scheduled task.
   */
  default void wrapInvocationAndInit(Invocator invocator) {
    try {
      invocator.run();
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  interface Invocator {
    void run() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException;

    default void runUnchecked() {
      try {
        run();
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new UncheckedException(e);
      }
    }

    /**
     * @return The full {@link Invocation} object, for use in {@link #wrapInvocation(Invocator)} and
     *     {@link #wrapInvocationAndInit(Invocator)}.
     */
    Invocation getInvocation();
  }

  /**
   * Fired when a transaction outbox task is successfully completed <em>and</em> recorded as such in
   * the database such that it will not be re-attempted. Note that:
   *
   * <ul>
   *   <li>{@link TransactionOutbox} uses "at least once" semantics, so the actual processing of a
   *       task may complete any number of times before this event is fired.
   *   <li>This event is not guaranteed to fire in the event of a JVM failure or power loss. It is
   *       fired <em>after</em> the commit to the database removing the completed task and all bets
   *       are off after this point.
   * </ul>
   *
   * @param entry The outbox entry completed.
   */
  default void success(TransactionOutboxEntry entry) {
    // No-op
  }

  /**
   * Fired when a transaction outbox task fails. This may occur multiple times until the maximum
   * number of retries, at which point this will be fired <em>and then</em> {@link
   * #blocked(TransactionOutboxEntry, Throwable)}. This event is not guaranteed to fire in the event
   * of a JVM failure or power loss. It is fired <em>after</em> the commit to the database marking
   * the task as failed.
   *
   * @param entry The outbox entry failed.
   * @param cause The cause of the most recent failure.
   */
  default void failure(TransactionOutboxEntry entry, Throwable cause) {
    // No-op
  }

  /**
   * Fired when a transaction outbox task has passed the maximum number of retries and has been
   * blocked. This event is not guaranteed to fire in the event of a JVM failure or power loss. It
   * is fired <em>after</em> the commit to the database marking the task as blocked.
   *
   * @param entry The outbox entry to be marked as blocked.
   * @param cause The cause of the most recent failure.
   */
  default void blocked(TransactionOutboxEntry entry, Throwable cause) {
    // No-op
  }

  /**
   * Implement this to provide session state that you want to include with a persisted request. This
   * is a free-form {@link Map} which you can then read in {@link #wrapInvocation(Invocator)} using
   * {@link Invocator#getInvocation()}.
   *
   * @return Your session information.
   */
  default Map<String, String> extractSession() {
    return null;
  }

  /**
   * Chains this listener with another and returns the result.
   *
   * @param other The other listener. It will always be called after this one.
   * @return The combined listener.
   */
  default TransactionOutboxListener andThen(TransactionOutboxListener other) {
    var self = this;
    return new TransactionOutboxListener() {

      @Override
      public void scheduled(TransactionOutboxEntry entry) {
        self.scheduled(entry);
        other.scheduled(entry);
      }

      @Override
      public void wrapInvocation(Invocator invocator)
          throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        self.wrapInvocation(
            new Invocator() {
              @Override
              public void run()
                  throws IllegalAccessException,
                      IllegalArgumentException,
                      InvocationTargetException {
                other.wrapInvocation(invocator);
              }

              @Override
              public Invocation getInvocation() {
                return invocator.getInvocation();
              }
            });
      }

      @Override
      public void wrapInvocationAndInit(Invocator invocator) {
        self.wrapInvocationAndInit(
            new Invocator() {
              @Override
              public void run() {
                other.wrapInvocationAndInit(invocator);
              }

              @Override
              public Invocation getInvocation() {
                return invocator.getInvocation();
              }
            });
      }

      @Override
      public Map<String, String> extractSession() {
        var mine = self.extractSession();
        var theirs = other.extractSession();
        if (mine == null) return theirs;
        if (theirs == null) return mine;
        Map<String, String> result = new HashMap<>(mine);
        result.putAll(theirs);
        return result;
      }

      @Override
      public void success(TransactionOutboxEntry entry) {
        self.success(entry);
        other.success(entry);
      }

      @Override
      public void failure(TransactionOutboxEntry entry, Throwable cause) {
        self.failure(entry, cause);
        other.failure(entry, cause);
      }

      @Override
      public void blocked(TransactionOutboxEntry entry, Throwable cause) {
        self.blocked(entry, cause);
        other.blocked(entry, cause);
      }
    };
  }
}
