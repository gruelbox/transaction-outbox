package com.gruelbox.transactionoutbox.jackson.acceptance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.gruelbox.transactionoutbox.*;
import com.gruelbox.transactionoutbox.jackson.JacksonInvocationSerializer;
import com.gruelbox.transactionoutbox.testing.AbstractAcceptanceTest;
import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
class TestJacksonSerializer extends AbstractAcceptanceTest {

  private final CountDownLatch latch = new CountDownLatch(1);

  @Override
  protected Persistor persistor() {
    return DefaultPersistor.builder()
        .dialect(connectionDetails().dialect())
        .serializer(
            JacksonInvocationSerializer.builder()
                .mapper(
                    new ObjectMapper()
                        .registerModule(new GuavaModule())
                        .registerModule(new Jdk8Module())
                        .registerModule(new JavaTimeModule())
                        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true))
                .build())
        .build();
  }

  void process(List<Object> difficultDataStructure) {
    assertEquals(List.of(LocalDate.of(2000, 1, 1), "a", "b", 2), difficultDataStructure);
  }

  @Test
  void testPolymorphicDeserialization() throws InterruptedException {
    var transactionManager = txManager();
    var outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(persistor())
            .instantiator(Instantiator.using(clazz -> TestJacksonSerializer.this))
            .listener(
                new TransactionOutboxListener() {
                  @Override
                  public void success(TransactionOutboxEntry entry) {
                    latch.countDown();
                  }
                })
            .build();
    transactionManager.inTransaction(
        () -> outbox.schedule(getClass()).process(List.of(LocalDate.of(2000, 1, 1), "a", "b", 2)));
    assertTrue(latch.await(2, TimeUnit.SECONDS));
  }
}
