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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
class TestJacksonSerializer extends AbstractAcceptanceTest {

  private final CountDownLatch latch = new CountDownLatch(1);
  private TransactionManager transactionManager;
  private TransactionOutbox outbox;

  @BeforeEach
  void beforeEach() {
    var mapper = new ObjectMapper();
    mapper.registerModule(new GuavaModule());
    mapper.registerModule(new Jdk8Module());
    mapper.registerModule(new JavaTimeModule());
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
    transactionManager = txManager();
    outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(
                DefaultPersistor.builder()
                    .dialect(Dialect.H2)
                    .serializer(JacksonInvocationSerializer.builder().mapper(mapper).build())
                    .build())
            .instantiator(Instantiator.using(clazz -> TestJacksonSerializer.this))
            .listener(
                new TransactionOutboxListener() {
                  @Override
                  public void success(TransactionOutboxEntry entry) {
                    latch.countDown();
                  }
                })
            .build();
  }

  void process(List<Object> difficultDataStructure) {
    assertEquals(List.of(LocalDate.of(2000, 1, 1), "a", "b", 2), difficultDataStructure);
  }

  @Test
  void testPolymorphicDeserialization() throws InterruptedException {
    transactionManager.inTransaction(
        () -> outbox.schedule(getClass()).process(List.of(LocalDate.of(2000, 1, 1), "a", "b", 2)));
    assertTrue(latch.await(2, TimeUnit.SECONDS));
  }
}
