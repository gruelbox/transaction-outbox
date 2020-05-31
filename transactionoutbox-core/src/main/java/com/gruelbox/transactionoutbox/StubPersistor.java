package com.gruelbox.transactionoutbox;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.Builder;

/** Stub implementation of {@link Persistor}. */
@Builder
public class StubPersistor implements Persistor {

  StubPersistor() {}

  @Override
  public void migrate(TransactionManager transactionManager) {
    // No-op
  }

  @Override
  public CompletableFuture<Void> save(Transaction tx, TransactionOutboxEntry entry) {
    return completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> delete(Transaction tx, TransactionOutboxEntry entry) {
    return completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> update(Transaction tx, TransactionOutboxEntry entry) {
    return completedFuture(null);
  }

  @Override
  public CompletableFuture<Boolean> lock(Transaction tx, TransactionOutboxEntry entry) {
    return completedFuture(true);
  }

  @Override
  public CompletableFuture<Boolean> whitelist(Transaction tx, String entryId) {
    return completedFuture(true);
  }

  @Override
  public CompletableFuture<List<TransactionOutboxEntry>> selectBatch(
      Transaction tx, int batchSize, Instant now) {
    return completedFuture(List.of());
  }

  @Override
  public CompletableFuture<Integer> deleteProcessedAndExpired(
      Transaction tx, int batchSize, Instant now) {
    return completedFuture(0);
  }

  @Override
  public CompletableFuture<Integer> clear(Transaction transaction) {
    return completedFuture(0);
  }
}
