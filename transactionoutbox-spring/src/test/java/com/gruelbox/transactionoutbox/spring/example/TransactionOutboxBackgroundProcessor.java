package com.gruelbox.transactionoutbox.spring.example;

import com.gruelbox.transactionoutbox.TransactionOutbox;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Simple implementation of a background processor for {@link TransactionOutbox}. You don't need to
 * use this if you need different semantics, but this is a good start for most purposes.
 */
@Component
@Slf4j
@RequiredArgsConstructor(onConstructor_ = {@Autowired})
class TransactionOutboxBackgroundProcessor {

  private final TransactionOutbox outbox;

  @Scheduled(fixedRateString = "${outbox.repeatEvery}")
  void poll() {
    try {
      do {
        log.info("Flushing");
      } while (outbox.flush());
    } catch (Exception e) {
      log.error("Error flushing transaction outbox. Pausing", e);
    }
  }
}
