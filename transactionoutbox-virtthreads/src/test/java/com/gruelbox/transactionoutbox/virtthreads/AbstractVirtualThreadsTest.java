package com.gruelbox.transactionoutbox.virtthreads;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gruelbox.transactionoutbox.*;
import com.gruelbox.transactionoutbox.testing.BaseTest;
import com.gruelbox.transactionoutbox.testing.InterfaceProcessor;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import me.escoffier.loom.loomunit.LoomUnitExtension;
import me.escoffier.loom.loomunit.ShouldNotPin;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@Slf4j
@ExtendWith(LoomUnitExtension.class)
abstract class AbstractVirtualThreadsTest extends BaseTest {

  private static final String VIRTUAL_THREAD_SCHEDULER_PARALLELISM =
      "jdk.virtualThreadScheduler.parallelism";

  @Test
  @ShouldNotPin
  final void highVolumeVirtualThreads() throws Exception {
    var count = 10;
    var latch = new CountDownLatch(count * 10);
    var transactionManager = txManager();
    var results = new ConcurrentHashMap<Integer, Integer>();
    var duplicates = new ConcurrentHashMap<Integer, Integer>();
    var outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(Persistor.forDialect(connectionDetails().dialect()))
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
}
