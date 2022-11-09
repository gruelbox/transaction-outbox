package com.synaos.transactionoutbox.acceptance;

import com.synaos.transactionoutbox.TransactionOutboxEntry;
import com.synaos.transactionoutbox.TransactionOutboxListener;
import lombok.Getter;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

/**
 * Collects an ordered list of all entry events (*excluding blocked events) that have hit this
 * listener until a specified number of blocks / successes have occurred.
 */
final class OrderedEntryListener implements TransactionOutboxListener {
    private final CountDownLatch successLatch;
    private final CountDownLatch blockedLatch;

    @Getter
    private volatile TransactionOutboxEntry blocked;

    private final CopyOnWriteArrayList<TransactionOutboxEntry> orderedEntries;

    OrderedEntryListener(CountDownLatch successLatch, CountDownLatch blockedLatch) {
        this.successLatch = successLatch;
        this.blockedLatch = blockedLatch;
        orderedEntries = new CopyOnWriteArrayList<>();
    }

    @Override
    public void scheduled(TransactionOutboxEntry entry) {
        orderedEntries.add(from(entry));
    }

    @Override
    public void success(TransactionOutboxEntry entry) {
        orderedEntries.add(from(entry));
        successLatch.countDown();
    }

    @Override
    public void failure(TransactionOutboxEntry entry, Throwable cause) {
        orderedEntries.add(from(entry));
    }

    @Override
    public void blocked(TransactionOutboxEntry entry, Throwable cause) {
        // due to the implementation of outbox (how it persists updates), it does not make sense to add
        // the blocked entry to the list for our current testing purposes.
        blocked = from(entry);
        blockedLatch.countDown();
    }

    /**
     * Retrieve an unmodifiable copy of {@link #orderedEntries}. Beware, expectation is that this does
     * not/ should not get accessed until the correct number of {@link
     * #success(TransactionOutboxEntry)} and {@link #blocked(TransactionOutboxEntry, Throwable)}}
     * counts have occurred.
     *
     * @return unmodifiable list of ordered outbox entry events.
     */
    public ImmutableList<TransactionOutboxEntry> getOrderedEntries() {
        return ImmutableList.copyOf(orderedEntries);
    }

    private TransactionOutboxEntry from(TransactionOutboxEntry entry) {
        return TransactionOutboxEntry.builder()
                .id(entry.getId())
                .uniqueRequestId(entry.getUniqueRequestId())
                .invocation(entry.getInvocation())
                .lastAttemptTime(entry.getLastAttemptTime())
                .nextAttemptTime(entry.getNextAttemptTime())
                .attempts(entry.getAttempts())
                .blocked(entry.isBlocked())
                .processed(entry.isProcessed())
                .version(entry.getVersion())
                .build();
    }
}
