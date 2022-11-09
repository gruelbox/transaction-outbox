package com.synaos.transactionoutbox.acceptance;

import com.synaos.transactionoutbox.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
