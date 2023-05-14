package com.gruelbox.transactionoutbox;

import static java.util.concurrent.CompletableFuture.completedFuture;

import com.gruelbox.transactionoutbox.spi.BaseTransaction;
import com.gruelbox.transactionoutbox.spi.BaseTransactionManager;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.Builder;

/** Stub implementation of {@link Persistor}. */
@Builder
public class StubPersistor implements Persistor {

  StubPersistor() {}

  @Override
  public void migrate(BaseTransactionManager transactionManager) {
    // No-op
  }

  @Override
  public CompletableFuture<Void> save(BaseTransaction tx, TransactionOutboxEntry entry) {
    return completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> delete(BaseTransaction tx, TransactionOutboxEntry entry) {
    return completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> update(BaseTransaction tx, TransactionOutboxEntry entry) {
    return completedFuture(null);
  }

  @Override
  public CompletableFuture<Boolean> lock(BaseTransaction tx, TransactionOutboxEntry entry) {
    return completedFuture(true);
  }

  @Override
  public CompletableFuture<Boolean> unblock(BaseTransaction tx, String entryId) {
    return completedFuture(true);
  }

  @Override
  public CompletableFuture<List<TransactionOutboxEntry>> selectBatch(
      BaseTransaction tx, int batchSize, Instant now) {
    return completedFuture(List.of());
  }

  @Override
  public CompletableFuture<Integer> deleteProcessedAndExpired(
      BaseTransaction tx, int batchSize, Instant now) {
    return completedFuture(0);
  }

  @Override
  public CompletableFuture<Integer> clear(BaseTransaction transaction) {
    return completedFuture(0);
  }

  @Override
  public CompletableFuture<Boolean> checkConnection(BaseTransaction baseTransaction) {
    return completedFuture(true);
  }
}
