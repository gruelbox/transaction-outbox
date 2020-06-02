package com.gruelbox.transactionoutbox.spi;

import com.gruelbox.transactionoutbox.Beta;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Beta
public class PersistorWrapper<CN, TX extends Transaction<CN, ?>> implements Persistor<CN, TX> {

  private final Persistor<CN, TX> delegate;

  public PersistorWrapper(Persistor<CN, TX> delegate) {
    this.delegate = delegate;
  }

  @Override
  public void migrate(TransactionManager<CN, ?, ? extends TX> tm) {
    delegate.migrate(tm);
  }

  @Override
  public CompletableFuture<Void> save(TX tx, TransactionOutboxEntry entry) {
    return delegate.save(tx, entry);
  }

  @Override
  public CompletableFuture<Void> delete(TX tx, TransactionOutboxEntry entry) {
    return delegate.delete(tx, entry);
  }

  @Override
  public CompletableFuture<Void> update(TX tx, TransactionOutboxEntry entry) {
    return delegate.update(tx, entry);
  }

  @Override
  public CompletableFuture<Boolean> lock(TX tx, TransactionOutboxEntry entry) {
    return delegate.lock(tx, entry);
  }

  @Override
  public CompletableFuture<Boolean> whitelist(TX tx, String entryId) {
    return delegate.whitelist(tx, entryId);
  }

  @Override
  public CompletableFuture<List<TransactionOutboxEntry>> selectBatch(
      TX tx, int batchSize, Instant now) {
    return delegate.selectBatch(tx, batchSize, now);
  }

  @Override
  public CompletableFuture<Integer> deleteProcessedAndExpired(TX tx, int batchSize, Instant now) {
    return delegate.deleteProcessedAndExpired(tx, batchSize, now);
  }

  @Override
  public CompletableFuture<Integer> clear(TX tx) {
    return delegate.clear(tx);
  }
}
