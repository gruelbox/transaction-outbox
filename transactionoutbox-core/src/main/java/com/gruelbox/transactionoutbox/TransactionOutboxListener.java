package com.gruelbox.transactionoutbox;

/** A listener for events fired by {@link TransactionOutbox}. */
public interface TransactionOutboxListener {

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
   * #blacklisted(TransactionOutboxEntry, Throwable)}. This event is not guaranteed to fire in the
   * event of a JVM failure or power loss. It is fired <em>after</em> the commit to the database
   * marking the task as failed.
   *
   * @param entry The outbox entry failed.
   * @param cause The cause of the most recent failure.
   */
  default void failure(TransactionOutboxEntry entry, Throwable cause) {
    // No-op
  }

  /**
   * Fired when a transaction outbox task has passed the maximum number of retries and has been
   * blacklisted. This event is not guaranteed to fire in the event of a JVM failure or power loss.
   * It is fired <em>after</em> the commit to the database marking the task as blacklisted.
   *
   * @param entry The outbox entry blacklisted.
   * @param cause The cause of the most recent failure.
   */
  default void blacklisted(TransactionOutboxEntry entry, Throwable cause) {
    // No-op
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
      public void blacklisted(TransactionOutboxEntry entry, Throwable cause) {
        self.blacklisted(entry, cause);
        other.blacklisted(entry, cause);
      }
    };
  }
}
