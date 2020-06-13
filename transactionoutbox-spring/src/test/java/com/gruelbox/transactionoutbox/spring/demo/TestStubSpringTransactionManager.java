package com.gruelbox.transactionoutbox.spring.demo;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.gruelbox.transactionoutbox.Instantiator;
import com.gruelbox.transactionoutbox.SpringTransactionManager;
import com.gruelbox.transactionoutbox.StubPersistor;
import com.gruelbox.transactionoutbox.StubSpringTransactionManager;
import com.gruelbox.transactionoutbox.Submitter;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import java.sql.Connection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
class TestStubSpringTransactionManager {

  private SpringTransactionManager transactionManager = new StubSpringTransactionManager();

  @Test
  void testStubSpringTransactionManager() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(StubPersistor.builder().build())
            .submitter(Submitter.withExecutor(Runnable::run))
            .instantiator(
                Instantiator.using(
                    clazz ->
                        (Runnable)
                            () -> {
                              log.info("Work triggered");
                              latch.countDown();
                            }))
            .build();

    transactionManager.inTransaction(
        tx -> {
          assertThat(tx.connection(), isA(Connection.class));
          log.info("Scheduling...");
          outbox.schedule(Runnable.class).run();
          log.info("Scheduled");
          try {
            // Should not be fired until after commit
            assertFalse(latch.await(2, TimeUnit.SECONDS));
          } catch (InterruptedException e) {
            fail("Interrupted");
          }
          log.info("Confirmed not fired, committing");
        });

    // Should be fired after commit
    assertTrue(latch.await(2, TimeUnit.SECONDS));
  }
}
