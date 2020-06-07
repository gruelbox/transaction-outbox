package com.gruelbox.transactionoutbox.jooq.acceptance;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.gruelbox.transactionoutbox.Instantiator;
import com.gruelbox.transactionoutbox.JooqTransaction;
import com.gruelbox.transactionoutbox.StubPersistor;
import com.gruelbox.transactionoutbox.StubThreadLocalJooqTransactionManager;
import com.gruelbox.transactionoutbox.Submitter;
import com.gruelbox.transactionoutbox.ThreadLocalJooqTransactionManager;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import java.sql.Connection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
class TestStubThreadLocalProvider {

  private ThreadLocalJooqTransactionManager transactionManager =
      new StubThreadLocalJooqTransactionManager();

  @Test
  void testSimpleDirectInvocationWithThreadContext() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(StubPersistor.builder().build())
            .submitter(Submitter.withExecutor(Runnable::run))
            .instantiator(Instantiator.using(clazz -> (Runnable) latch::countDown))
            .build();

    transactionManager.inTransaction(
        () -> {
          JooqTransaction jooqTransaction = transactionManager.requireTransactionReturns(tx -> tx);
          assertThat(jooqTransaction.connection(), isA(Connection.class));

          outbox.schedule(Runnable.class).run();
          try {
            // Should not be fired until after commit
            assertFalse(latch.await(2, TimeUnit.SECONDS));
          } catch (InterruptedException e) {
            fail("Interrupted");
          }
        });

    // Should be fired after commit
    assertTrue(latch.await(2, TimeUnit.SECONDS));
  }
}
