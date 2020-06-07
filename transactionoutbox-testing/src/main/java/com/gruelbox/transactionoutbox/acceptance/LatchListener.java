package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.TransactionOutboxListener;
import com.gruelbox.transactionoutbox.Utils;
import java.util.concurrent.CountDownLatch;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.event.Level;

@Slf4j
public final class LatchListener implements TransactionOutboxListener {
  private final CountDownLatch successLatch;
  private final Level logLevel;
  private final CountDownLatch blacklistLatch;

  @Getter private volatile TransactionOutboxEntry blacklisted;

  public LatchListener(CountDownLatch successLatch, CountDownLatch blacklistLatch) {
    this.successLatch = successLatch;
    this.logLevel = Level.DEBUG;
    this.blacklistLatch = blacklistLatch;
  }

  public LatchListener(CountDownLatch successLatch) {
    this.successLatch = successLatch;
    this.logLevel = Level.DEBUG;
    this.blacklistLatch = new CountDownLatch(1);
  }

  public LatchListener(CountDownLatch successLatch, Level logLevel) {
    this.successLatch = successLatch;
    this.logLevel = logLevel;
    this.blacklistLatch = new CountDownLatch(1);
  }

  @Override
  public void success(TransactionOutboxEntry entry) {
    Utils.logAtLevel(log, logLevel, "Got success: {}", entry);
    successLatch.countDown();
  }

  @Override
  public void blacklisted(TransactionOutboxEntry entry, Throwable cause) {
    Utils.logAtLevel(log, logLevel, "Got blackisting: {}", entry);
    this.blacklisted = entry;
    blacklistLatch.countDown();
  }

  @Override
  public void failure(TransactionOutboxEntry entry, Throwable cause) {
    Utils.logAtLevel(log, logLevel, "Got failure: {}", entry);
  }
}
