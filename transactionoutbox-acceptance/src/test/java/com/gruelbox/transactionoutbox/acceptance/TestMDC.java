package com.gruelbox.transactionoutbox.acceptance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gruelbox.transactionoutbox.*;
import com.gruelbox.transactionoutbox.testing.InterfaceProcessor;
import com.gruelbox.transactionoutbox.testing.LatchListener;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

@Slf4j
class TestMDC {

  @Test
  final void testMDCPassedToTask() throws InterruptedException {

    TransactionManager transactionManager = new StubThreadLocalTransactionManager();

    CountDownLatch latch = new CountDownLatch(1);
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .instantiator(
                Instantiator.using(
                    clazz ->
                        (InterfaceProcessor)
                            (foo, bar) -> {
                              log.info("Processing ({}, {})", foo, bar);
                              assertEquals("Foo", MDC.get("SESSION-KEY"));
                            }))
            .listener(new LatchListener(latch))
            .persistor(StubPersistor.builder().build())
            .build();

    MDC.put("SESSION-KEY", "Foo");
    try {
      transactionManager.inTransaction(
          () -> outbox.schedule(InterfaceProcessor.class).process(3, "Whee"));
    } finally {
      MDC.clear();
    }

    assertTrue(latch.await(2, TimeUnit.SECONDS));
  }
}
