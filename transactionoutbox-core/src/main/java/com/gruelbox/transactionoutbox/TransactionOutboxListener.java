package com.gruelbox.transactionoutbox;

/** A listener for events fired by {@link TransactionOutbox}. */
public interface TransactionOutboxListener {

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
  void success(TransactionOutboxEntry entry);
}
