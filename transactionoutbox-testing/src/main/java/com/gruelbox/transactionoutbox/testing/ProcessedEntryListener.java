package com.gruelbox.transactionoutbox.testing;

import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.TransactionOutboxListener;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import lombok.extern.slf4j.Slf4j;

/**
 * Collects an ordered list of tx outbox entries that have been 'processed' i.e. succeeded or failed
 * in processing.
 */
@Slf4j
public final class ProcessedEntryListener implements TransactionOutboxListener {
  private final CountDownLatch successLatch;

  private final CopyOnWriteArrayList<TransactionOutboxEntry> successfulEntries =
      new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<TransactionOutboxEntry> failingEntries =
      new CopyOnWriteArrayList<>();

  public ProcessedEntryListener(CountDownLatch successLatch) {
    this.successLatch = successLatch;
  }

  @Override
  public void success(TransactionOutboxEntry entry) {
    var copy = from(entry);
    successfulEntries.add(copy);
    log.info(
        "Received success #{}. Counting down at {}",
        successfulEntries.size(),
        successLatch.getCount());
    successLatch.countDown();
  }

  @Override
  public void failure(TransactionOutboxEntry entry, Throwable cause) {
    failingEntries.add(from(entry));
  }

  /**
   * Retrieve an unmodifiable copy of {@link #successfulEntries}. Beware, expectation is that this
   * does not/ should not get accessed until the correct number of {@link
   * #success(TransactionOutboxEntry)} and {@link #blocked(TransactionOutboxEntry, Throwable)}}
   * counts have occurred.
   *
   * @return unmodifiable list of ordered outbox entry events.
   */
  public List<TransactionOutboxEntry> getSuccessfulEntries() {
    return List.copyOf(successfulEntries);
  }

  public List<TransactionOutboxEntry> getFailingEntries() {
    return List.copyOf(failingEntries);
  }

  private TransactionOutboxEntry from(TransactionOutboxEntry entry) {
    return entry.toBuilder().build();
  }
}
