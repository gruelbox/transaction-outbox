package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.TransactionOutboxListener;
import java.util.concurrent.CountDownLatch;
import lombok.Getter;

final class LatchListener implements TransactionOutboxListener {
  private final CountDownLatch successLatch;
  private final CountDownLatch blockedLatch;

  @Getter private volatile TransactionOutboxEntry blocked;

  LatchListener(CountDownLatch successLatch, CountDownLatch markFailedLatch) {
    this.successLatch = successLatch;
    this.blockedLatch = markFailedLatch;
  }

  LatchListener(CountDownLatch successLatch) {
    this.successLatch = successLatch;
    this.blockedLatch = new CountDownLatch(1);
  }

  @Override
  public void success(TransactionOutboxEntry entry) {
    successLatch.countDown();
  }

  @Override
  public void blocked(TransactionOutboxEntry entry, Throwable cause) {
    this.blocked = entry;
    blockedLatch.countDown();
  }
}
