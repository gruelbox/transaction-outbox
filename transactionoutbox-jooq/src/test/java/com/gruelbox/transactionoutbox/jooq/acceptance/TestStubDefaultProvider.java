package com.gruelbox.transactionoutbox.jooq.acceptance;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.gruelbox.transactionoutbox.DefaultJooqTransactionManager;
import com.gruelbox.transactionoutbox.Instantiator;
import com.gruelbox.transactionoutbox.JooqTransaction;
import com.gruelbox.transactionoutbox.StubDefaultJooqTransactionManager;
import com.gruelbox.transactionoutbox.StubPersistor;
import com.gruelbox.transactionoutbox.Submitter;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import java.sql.Connection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.jooq.Configuration;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

@Slf4j
class TestStubDefaultProvider {

  private DefaultJooqTransactionManager transactionManager =
      new StubDefaultJooqTransactionManager();

  @Test
  void testStubWithTransaction() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(StubPersistor.builder().build())
            .submitter(Submitter.withExecutor(Runnable::run))
            .logLevelTemporaryFailure(Level.DEBUG)
            .logLevelProcessStartAndFinish(Level.DEBUG)
            .instantiator(
                Instantiator.using(
                    clazz ->
                        (TransactionUser)
                            tx -> {
                              assertNotNull(tx);
                              assertThat(tx.connection(), isA(Connection.class));
                              assertThat(tx.context(), isA(Configuration.class));
                              latch.countDown();
                            }))
            .build();

    transactionManager.inTransaction(
        tx -> {
          outbox.schedule(TransactionUser.class).run(tx);
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

  @Test
  void testStubWithContext() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(StubPersistor.builder().build())
            .submitter(Submitter.withExecutor(Runnable::run))
            .logLevelTemporaryFailure(Level.DEBUG)
            .logLevelProcessStartAndFinish(Level.DEBUG)
            .instantiator(
                Instantiator.using(
                    clazz ->
                        (ContextUser)
                            cfg -> {
                              assertNotNull(cfg);
                              assertThat(cfg, isA(Configuration.class));
                              latch.countDown();
                            }))
            .build();

    transactionManager.inTransaction(
        tx -> {
          assertThat(tx.context(), isA(Configuration.class));
          outbox.schedule(ContextUser.class).run(tx.context());
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

  interface TransactionUser {
    void run(JooqTransaction transaction);
  }

  interface ContextUser {
    void run(Configuration cfg);
  }
}
