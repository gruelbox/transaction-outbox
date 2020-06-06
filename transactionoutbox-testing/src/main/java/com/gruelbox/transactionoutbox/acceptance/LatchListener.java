package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.TransactionOutboxListener;
import java.util.concurrent.CountDownLatch;
import lombok.Getter;

public final class LatchListener implements TransactionOutboxListener {
  private final CountDownLatch successLatch;
  private final CountDownLatch blacklistLatch;

  @Getter private volatile TransactionOutboxEntry blacklisted;

  LatchListener(CountDownLatch successLatch, CountDownLatch blacklistLatch) {
    this.successLatch = successLatch;
    this.blacklistLatch = blacklistLatch;
  }

  LatchListener(CountDownLatch successLatch) {
    this.successLatch = successLatch;
    this.blacklistLatch = new CountDownLatch(1);
  }

  @Override
  public void success(TransactionOutboxEntry entry) {
    successLatch.countDown();
  }

  @Override
  public void blacklisted(TransactionOutboxEntry entry, Throwable cause) {
    this.blacklisted = entry;
    blacklistLatch.countDown();
  }
}
