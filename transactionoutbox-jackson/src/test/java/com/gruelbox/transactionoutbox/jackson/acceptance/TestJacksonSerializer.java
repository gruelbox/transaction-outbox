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
import com.gruelbox.transactionoutbox.testing.LatchListener;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
class TestJacksonSerializer extends AbstractAcceptanceTest {

  private final CountDownLatch latch = new CountDownLatch(1);
  private final ThreadLocal<Map<String, String>> sessionVariable = new ThreadLocal<>();

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
    assertEquals(Map.of("sessionVar", "foobar"), sessionVariable.get());
  }

  @Test
  void testPolymorphicDeserialization() throws Exception {
    var transactionManager = txManager();
    var outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(persistor())
            .instantiator(Instantiator.using(clazz -> TestJacksonSerializer.this))
            .listener(
                new LatchListener(latch)
                    .andThen(
                        new TransactionOutboxListener() {
                          @Override
                          public Map<String, String> extractSession() {
                            var session = new HashMap<String, String>();
                            session.put("sessionVar", "foobar");
                            return session;
                          }

                          @Override
                          public void wrapInvocationAndInit(Invocator invocator) {
                            sessionVariable.set(invocator.getInvocation().getSession());
                            try {
                              invocator.runUnchecked();
                            } finally {
                              sessionVariable.remove();
                            }
                          }
                        }))
            .build();
    transactionManager.inTransaction(
        () -> outbox.schedule(getClass()).process(List.of(LocalDate.of(2000, 1, 1), "a", "b", 2)));
    assertTrue(latch.await(2, TimeUnit.SECONDS));
  }
}
