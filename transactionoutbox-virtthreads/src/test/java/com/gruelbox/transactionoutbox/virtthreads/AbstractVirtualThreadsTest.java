package com.gruelbox.transactionoutbox.virtthreads;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gruelbox.transactionoutbox.*;
import com.gruelbox.transactionoutbox.testing.BaseTest;
import com.gruelbox.transactionoutbox.testing.InterfaceProcessor;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
abstract class AbstractVirtualThreadsTest extends BaseTest {

  @Test
  final void highVolumeVirtualThreads() throws Exception {
    var count = 10;
    var latch = new CountDownLatch(count * 10);
    var transactionManager = txManager();
    var results = new ConcurrentHashMap<Integer, Integer>();
    var duplicates = new ConcurrentHashMap<Integer, Integer>();
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var outbox =
          TransactionOutbox.builder()
              .transactionManager(transactionManager)
              .persistor(Persistor.forDialect(connectionDetails().dialect()))
              .instantiator(Instantiator.using(clazz -> (InterfaceProcessor) (foo, bar) -> {}))
              .submitter(Submitter.withExecutor(executor))
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

      withRunningFlusher(
          outbox,
          () -> {
            var futures =
                IntStream.range(0, count)
                    .mapToObj(
                        i ->
                            CompletableFuture.runAsync(
                                () ->
                                    transactionManager.inTransaction(
                                        () -> {
                                          for (int j = 0; j < 10; j++) {
                                            outbox
                                                .schedule(InterfaceProcessor.class)
                                                .process(i * 10 + j, "Whee");
                                          }
                                        }),
                                executor))
                    .toArray(CompletableFuture[]::new);
            CompletableFuture.allOf(futures).join();
            assertTrue(latch.await(30, TimeUnit.SECONDS), "Latch not opened in time");
          });

      assertThat(
          "Should never get duplicates running to full completion", duplicates.keySet(), empty());
      assertThat(
          "Only got: " + results.keySet(),
          results.keySet(),
          containsInAnyOrder(IntStream.range(0, count * 10).boxed().toArray()));
    }
  }
}
