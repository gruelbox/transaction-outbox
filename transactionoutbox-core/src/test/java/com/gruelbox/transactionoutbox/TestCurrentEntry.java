package com.gruelbox.transactionoutbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.lang.reflect.InvocationTargetException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class TestCurrentEntry {

  private final Map<String, TransactionOutboxEntry> currentEntryAt = new LinkedHashMap<>();

  private String scheduledEntryId;

  @Test
  void exposesRunningEntryAtEverySpiEntryPointInvokedDuringProcessing() {
    StubThreadLocalTransactionManager transactionManager = new StubThreadLocalTransactionManager();
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .instantiator(instantiator())
            .persistor(persistor())
            .submitter(Submitter.withExecutor(Runnable::run))
            .transactionManager(transactionManager)
            .listener(listener())
            .build();

    transactionManager.inTransaction(() -> outbox.schedule(Task.class).run("foo"));

    assertNull(currentEntryAt.remove("listener.scheduled"), "Not processing yet");
    assertEquals(
        Set.of(
            "listener.wrapInvocationAndInit",
            "persistor.lock",
            "instantiator.getInstance",
            "listener.wrapInvocation",
            "task",
            "listener.success"),
        currentEntryAt.keySet());
    currentEntryAt.forEach(
        (spiEntryPoint, entry) -> {
          assertNotNull(entry, spiEntryPoint);
          assertEquals(scheduledEntryId, entry.getId(), spiEntryPoint);
          assertEquals("foo", entry.getInvocation().getArgs()[0], spiEntryPoint);
        });
    assertNotSame(currentEntryAt.get("task"), currentEntryAt.get("instantiator.getInstance"));
    assertNull(TransactionOutboxEntry.current(), "Cleared after processing");
  }

  private void record(String spiEntryPoint) {
    currentEntryAt.put(spiEntryPoint, TransactionOutboxEntry.current());
  }

  private Instantiator instantiator() {
    return new Instantiator() {

      @Override
      public String getName(Class<?> clazz) {
        return clazz.getName();
      }

      @Override
      public Object getInstance(String name) {
        record("instantiator.getInstance");
        return new Task();
      }
    };
  }

  private Persistor persistor() {
    return new StubPersistor() {
      @Override
      public boolean lock(Transaction tx, TransactionOutboxEntry entry) {
        record("persistor.lock");
        return super.lock(tx, entry);
      }
    };
  }

  private TransactionOutboxListener listener() {
    return new TransactionOutboxListener() {

      @Override
      public void scheduled(TransactionOutboxEntry entry) {
        scheduledEntryId = entry.getId();
        record("listener.scheduled");
      }

      @Override
      public void wrapInvocationAndInit(Invocator invocator) {
        record("listener.wrapInvocationAndInit");
        TransactionOutboxListener.super.wrapInvocationAndInit(invocator);
      }

      @Override
      public void wrapInvocation(Invocator invocator)
          throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        record("listener.wrapInvocation");
        TransactionOutboxListener.super.wrapInvocation(invocator);
      }

      @Override
      public void success(TransactionOutboxEntry entry) {
        record("listener.success");
      }
    };
  }

  class Task {

    @SuppressWarnings("unused")
    void run(String argument) {
      record("task");
    }
  }
}
