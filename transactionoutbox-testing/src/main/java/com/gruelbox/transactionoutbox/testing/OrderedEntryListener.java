package com.gruelbox.transactionoutbox.testing;

import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.TransactionOutboxListener;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Collects an ordered list of all entry events (*excluding blocked events) that have hit this
 * listener until a specified number of blocks / successes have occurred.
 */
@Slf4j
public final class OrderedEntryListener implements TransactionOutboxListener {
  private final CountDownLatch successLatch;
  private final CountDownLatch blockedLatch;

  @Getter private volatile TransactionOutboxEntry blocked;

  private final CopyOnWriteArrayList<TransactionOutboxEntry> events = new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<TransactionOutboxEntry> successes =
      new CopyOnWriteArrayList<>();

  OrderedEntryListener(CountDownLatch successLatch, CountDownLatch blockedLatch) {
    this.successLatch = successLatch;
    this.blockedLatch = blockedLatch;
  }

  @Override
  public void scheduled(TransactionOutboxEntry entry) {
    events.add(from(entry));
  }

  @Override
  public void success(TransactionOutboxEntry entry) {
    var copy = from(entry);
    events.add(copy);
    successes.add(copy);
    log.info(
        "Received success #{}. Counting down at {}", successes.size(), successLatch.getCount());
    successLatch.countDown();
  }

  @Override
  public void failure(TransactionOutboxEntry entry, Throwable cause) {
    events.add(from(entry));
  }

  @Override
  public void blocked(TransactionOutboxEntry entry, Throwable cause) {
    // due to the implementation of outbox (how it persists updates), it does not make sense to add
    // the blocked entry to the list for our current testing purposes.
    blocked = from(entry);
    blockedLatch.countDown();
  }

  /**
   * Retrieve an unmodifiable copy of {@link #events}. Beware, expectation is that this does not/
   * should not get accessed until the correct number of {@link #success(TransactionOutboxEntry)}
   * and {@link #blocked(TransactionOutboxEntry, Throwable)}} counts have occurred.
   *
   * @return unmodifiable list of ordered outbox entry events.
   */
  public List<TransactionOutboxEntry> getEvents() {
    return List.copyOf(events);
  }

  public List<TransactionOutboxEntry> getSuccesses() {
    return List.copyOf(successes);
  }

  private TransactionOutboxEntry from(TransactionOutboxEntry entry) {
    return entry.toBuilder().build();
  }
}
