package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.TransactionOutboxListener;
import java.util.concurrent.CountDownLatch;
import lombok.Getter;

final class LatchListener implements TransactionOutboxListener {
  private final CountDownLatch successLatch;
  private final CountDownLatch markFailedLatch;

  @Getter private volatile TransactionOutboxEntry failed;

  LatchListener(CountDownLatch successLatch, CountDownLatch markFailedLatch) {
    this.successLatch = successLatch;
    this.markFailedLatch = markFailedLatch;
  }

  LatchListener(CountDownLatch successLatch) {
    this.successLatch = successLatch;
    this.markFailedLatch = new CountDownLatch(1);
  }

  @Override
  public void success(TransactionOutboxEntry entry) {
    successLatch.countDown();
  }

  @Override
  public void markFailed(TransactionOutboxEntry entry, Throwable cause) {
    this.failed = entry;
    markFailedLatch.countDown();
  }
}
