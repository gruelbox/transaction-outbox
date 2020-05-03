package com.gruelbox.transactionoutbox;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PACKAGE)
class ExecutorSubmitter implements Submitter {

  private final Executor executor;

  @Override
  public void submit(TransactionOutboxEntry entry, Consumer<TransactionOutboxEntry> localExecutor) {
    try {
      executor.execute(() -> localExecutor.accept(entry));
      log.debug("Submitted {} for immediate processing", entry.description());
    } catch (RejectedExecutionException e) {
      log.debug("Queued {} for processing when executor is available", entry.description());
    } catch (Exception e) {
      log.warn(
          "Failed to submit {} for execution. It will be re-attempted later.",
          entry.description(),
          e);
    }
  }
}
