package com.gruelbox.transactionoutbox.virtthreads;

import static java.util.stream.Collectors.joining;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.gruelbox.transactionoutbox.*;
import com.gruelbox.transactionoutbox.testing.BaseTest;
import com.gruelbox.transactionoutbox.testing.InterfaceProcessor;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingStream;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
abstract class AbstractVirtualThreadsTest extends BaseTest {

  private static final String VIRTUAL_THREAD_SCHEDULER_PARALLELISM =
      "jdk.virtualThreadScheduler.parallelism";

  protected RecordingStream stream;
  private final AtomicBoolean alerted = new AtomicBoolean();
  private final CountDownLatch pinLatch = new CountDownLatch(1);

  @BeforeEach
  final void beforeEachAbstractVirtualThreadsTest() throws InterruptedException {
    stream = new RecordingStream();
    stream.enable("jdk.VirtualThreadPinned").withThreshold(Duration.ZERO);
    stream.onEvent(
        "jdk.VirtualThreadPinned",
        event -> {
          if (isTransientInitialization(event)) {
            log.info("Ignored classloader issues since these are transient...");
            return;
          }
          log.info(
              """
        Pinning Event Captured:
        Reason: %s
        Blocking operation: %s
        Duration: %dms
        Stack trace:
        %s"""
                  .formatted(
                      event.getValue("pinnedReason"),
                      event.getValue("blockingOperation"),
                      event.getDuration().toMillis(),
                      event.getStackTrace().getFrames().stream()
                          .map(
                              f ->
                                  " - %s %s.%s(%s)"
                                      .formatted(
                                          f.getType(),
                                          f.getMethod().getType().getName(),
                                          f.getMethod().getName(),
                                          f.getLineNumber()))
                          .collect(joining("\n"))));
          pinLatch.countDown();
        });
    stream.startAsync();
    Thread.sleep(500);
  }

  private boolean isTransientInitialization(RecordedEvent event) {
    var stackTrace = event.getStackTrace();
    if (stackTrace == null) return false;

    return stackTrace.getFrames().stream()
        .anyMatch(
            frame -> {
              String method = frame.getMethod().getType().getName();
              return method.contains("jdk.internal.loader")
                  || method.contains("java.lang.ClassLoader")
                  || method.contains("java.lang.invoke.MethodHandles");
            });
  }

  @AfterEach
  final void afterEachAbstractVirtualThreadsTest() {
    try {
      if (didPin()) {
        fail("Virtual thread was pinned. See earlier messages");
      }
    } finally {
      stream.close();
    }
  }

  protected boolean didPin() {
    try {
      if (alerted.getAndSet(true)) {
        log.info("Suppressed pinning alert as already checked");
        return false;
      }
      if (!pinLatch.await(5, TimeUnit.SECONDS)) {
        return false;
      }
      return true;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  final void highVolumeVirtualThreads() throws Exception {
    var count = 10;
    var latch = new CountDownLatch(count * 10);
    var transactionManager = txManager();
    var results = new ConcurrentHashMap<Integer, Integer>();
    var duplicates = new ConcurrentHashMap<Integer, Integer>();
    var persistor = Persistor.forDialect(connectionDetails().dialect());
    var outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(persistor)
            .instantiator(Instantiator.using(clazz -> (InterfaceProcessor) (foo, bar) -> {}))
            .submitter(
                Submitter.withExecutor(
                    r -> Thread.ofVirtual().name(UUID.randomUUID().toString()).start(r)))
            .attemptFrequency(Duration.ofMillis(500))
            .flushBatchSize(1000)
            .listener(
                new TransactionOutboxListener() {
                  @Override
                  public void success(TransactionOutboxEntry entry) {
                    Integer i = (Integer) entry.getInvocation().getArgs()[0];
                    if (results.putIfAbsent(i, i) != null) {
                      duplicates.put(i, i);
                    }
                    latch.countDown();
                  }
                })
            .build();

    warmupJdk(transactionManager, persistor);

    var parallelism = System.getProperty(VIRTUAL_THREAD_SCHEDULER_PARALLELISM);
    System.setProperty(VIRTUAL_THREAD_SCHEDULER_PARALLELISM, "1");
    try {
      withRunningFlusher(
          outbox,
          () -> {
            var futures =
                IntStream.range(0, count)
                    .mapToObj(
                        i ->
                            new FutureTask<Void>(
                                () ->
                                    transactionManager.inTransaction(
                                        () -> {
                                          for (int j = 0; j < 10; j++) {
                                            outbox
                                                .schedule(InterfaceProcessor.class)
                                                .process(i * 10 + j, "Whee");
                                          }
                                        }),
                                null))
                    .toList();
            futures.forEach(Thread::startVirtualThread);
            for (var future : futures) {
              future.get(20, TimeUnit.SECONDS);
            }
            assertTrue(latch.await(30, TimeUnit.SECONDS), "Latch not opened in time");
          });
    } finally {
      if (parallelism == null) {
        System.clearProperty(VIRTUAL_THREAD_SCHEDULER_PARALLELISM);
      } else {
        System.setProperty(VIRTUAL_THREAD_SCHEDULER_PARALLELISM, parallelism);
      }
    }

    assertThat(
        "Should never get duplicates running to full completion", duplicates.keySet(), empty());
    assertThat(
        "Only got: " + results.keySet(),
        results.keySet(),
        containsInAnyOrder(IntStream.range(0, count * 10).boxed().toArray()));
  }

  private void warmupJdk(TransactionManager transactionManager, DefaultPersistor persistor)
      throws NoSuchMethodException,
          IllegalAccessException,
          InvocationTargetException,
          SQLException {
    // Warm up Invocation.invoke so it converts to a MethodHandle now rather than later (which
    // causes pinning)
    for (int i = 0; i < 50; i++) {
      var invocation =
          new Invocation(
              InterfaceProcessor.class.getName(),
              "process",
              new Class<?>[] {int.class, String.class},
              new Object[] {1, ""});
      InterfaceProcessor warmupTarget =
          (foo, bar) -> {
            log.info("Warmup");
          };
      var invokeMethod =
          Invocation.class.getDeclaredMethod(
              "invoke", Object.class, TransactionOutboxListener.class);
      invokeMethod.setAccessible(true);
      invokeMethod.invoke(invocation, warmupTarget, new TransactionOutboxListener() {});
    }

    // And do a bit of database access now to warm up the JDBC driver
    transactionManager.inTransactionThrows(
        tx -> {
          Assert.assertTrue(persistor.checkConnection(tx));
          persistor.clear(tx);
        });
  }
}
