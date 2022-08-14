package com.gruelbox.transactionoutbox.acceptance;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.gruelbox.transactionoutbox.*;
import com.gruelbox.transactionoutbox.jackson.JacksonInvocationSerializer;
import com.gruelbox.transactionoutbox.sql.Dialects;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestJackson extends AbstractSimpleTransactionManagerAcceptanceTest {
  @Override
  protected boolean includeLongRunningTests() {
    return false;
  }

  @Override
  protected JdbcConnectionDetails connectionDetails() {
    return JdbcConnectionDetails.builder()
            .dialect(Dialects.H2)
            .driverClassName("org.h2.Driver")
            .url(
                    "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;DEFAULT_LOCK_TIMEOUT=60000;LOB_TIMEOUT=2000;MV_STORE=TRUE")
            .user("test")
            .password("test")
            .build();
  }

  @Override
  protected InvocationSerializer serializer() {
    var mapper = new ObjectMapper();
    mapper.registerModule(new GuavaModule());
    mapper.registerModule(new Jdk8Module());
    mapper.registerModule(new JavaTimeModule());
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
    return JacksonInvocationSerializer.builder().mapper(mapper).build();
  }

  @Test
  final void testPolymorphicDeserialization() {
    CountDownLatch latch = new CountDownLatch(1);
    CountDownLatch chainCompleted = new CountDownLatch(1);
    AtomicBoolean scheduled = new AtomicBoolean();
    TransactionOutbox outbox =
            builder()
                    .instantiator(new LoggingInstantiator())
                    .listener(
                            new LatchListener(latch, Level.INFO)
                                    .andThen(
                                            new TransactionOutboxListener() {
                                              @Override
                                              public void scheduled(TransactionOutboxEntry entry) {
                                                scheduled.set(true);
                                              }
                                            }))
                    .initializeImmediately(false)
                    .build();

    outbox.initialize();
    cleanDataStore();

    Utils.join(
            txManager
                    .transactionally(
                            tx ->
                                    outbox.schedule(getClass()).process(List.of(LocalDate.of(2000, 1, 1), "a", "b", 2)))
                    .thenRunAsync(() -> assertFired(latch))
                    .thenRun(chainCompleted::countDown));

    assertFired(chainCompleted);
    assertTrue(scheduled.get());
  }

  CompletableFuture<Void> process(List<Object> difficultDataStructure) {
    assertEquals(List.of(LocalDate.of(2000, 1, 1), "a", "b", 2), difficultDataStructure);
    return CompletableFuture.completedFuture(null);
  }
}
