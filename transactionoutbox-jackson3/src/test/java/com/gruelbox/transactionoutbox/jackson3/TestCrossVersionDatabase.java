package com.gruelbox.transactionoutbox.jackson3;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.gruelbox.transactionoutbox.*;
import com.gruelbox.transactionoutbox.testing.BaseTest;
import com.gruelbox.transactionoutbox.testing.LatchListener;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class TestCrossVersionDatabase extends BaseTest {

  private static final Instant INSTANT = Instant.parse("2024-05-01T10:15:30.123456Z");

  private final AtomicReference<List<Object>> received = new AtomicReference<>();

  void process(List<Object> values, Color color, Instant instant) {
    received.set(List.of(values, color, instant));
  }

  @Test
  void taskScheduledByJackson2IsProcessedByJackson3() throws Exception {
    scheduleThenProcess(jackson2Persistor(), jackson3Persistor());
  }

  @Test
  void taskScheduledByJackson3IsProcessedByJackson2() throws Exception {
    scheduleThenProcess(jackson3Persistor(), jackson2Persistor());
  }

  private void scheduleThenProcess(Persistor writer, Persistor reader) throws Exception {
    TransactionManager transactionManager = txManager();

    // persists the task and never execute it
    TransactionOutbox scheduling =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(writer)
            .submitter(Submitter.withExecutor(task -> {}))
            .build();
    clearOutbox();
    transactionManager.inTransaction(
        () ->
            scheduling
                .schedule(getClass())
                .process(List.of(LocalDate.of(2000, 1, 1), "a", 2), Color.GREEN, INSTANT));

    //  picks the stored row
    CountDownLatch latch = new CountDownLatch(1);
    TransactionOutbox processing =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(reader)
            .instantiator(Instantiator.using(clazz -> this))
            .listener(new LatchListener(latch))
            .build();
    withRunningFlusher(processing, () -> assertTrue(latch.await(5, TimeUnit.SECONDS)));

    assertEquals(
        List.of(List.of(LocalDate.of(2000, 1, 1), "a", 2), Color.GREEN, INSTANT), received.get());
  }

  private Persistor jackson2Persistor() {
    return DefaultPersistor.builder()
        .dialect(connectionDetails().dialect())
        .serializer(
            com.gruelbox.transactionoutbox.jackson.JacksonInvocationSerializer.builder()
                .mapper(
                    new com.fasterxml.jackson.databind.ObjectMapper()
                        .registerModule(new JavaTimeModule()))
                .build())
        .build();
  }

  private Persistor jackson3Persistor() {
    return DefaultPersistor.builder()
        .dialect(connectionDetails().dialect())
        .serializer(
            JacksonInvocationSerializer.builder().mapper(JsonMapper.builder().build()).build())
        .build();
  }
}
