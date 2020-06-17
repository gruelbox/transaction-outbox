package com.gruelbox.transactionoutbox.acceptance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.gruelbox.transactionoutbox.Instantiator;
import com.gruelbox.transactionoutbox.StubPersistor;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import com.gruelbox.transactionoutbox.jdbc.JdbcTransaction;
import com.gruelbox.transactionoutbox.jdbc.SimpleTransaction;
import com.gruelbox.transactionoutbox.jdbc.SimpleTransactionManager;
import com.gruelbox.transactionoutbox.jdbc.StubSimpleTransactionManager;
import java.sql.Connection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.MDC;

@Slf4j
class TestMDC {

  @Test
  final void testMDCPassedToTask() throws InterruptedException {

    SimpleTransactionManager transactionManager =
        new StubSimpleTransactionManager(
            () -> new SimpleTransaction<>(Mockito.mock(Connection.class), null));

    CountDownLatch latch = new CountDownLatch(1);
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .instantiator(
                Instantiator.using(
                    clazz ->
                        new BlockingInterfaceProcessor() {
                          @Override
                          public void process(int foo, String bar) {
                            log.info("Processing ({}, {})", foo, bar);
                            assertEquals("Foo", MDC.get("SESSION-KEY"));
                          }

                          @Override
                          public void process(int foo, String bar, JdbcTransaction transaction) {
                            throw new UnsupportedOperationException();
                          }
                        }))
            .listener(new LatchListener(latch))
            .persistor(StubPersistor.builder().build())
            .build();

    MDC.put("SESSION-KEY", "Foo");
    try {
      transactionManager.inTransaction(
          () -> outbox.schedule(BlockingInterfaceProcessor.class).process(3, "Whee"));
    } finally {
      MDC.clear();
    }

    assertTrue(latch.await(2, TimeUnit.SECONDS));
  }
}
