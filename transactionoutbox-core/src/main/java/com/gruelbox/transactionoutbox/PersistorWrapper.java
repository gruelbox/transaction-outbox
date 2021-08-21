package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.spi.BaseTransaction;
import com.gruelbox.transactionoutbox.spi.BaseTransactionManager;
import com.gruelbox.transactionoutbox.spi.InitializationEventBus;
import com.gruelbox.transactionoutbox.spi.InitializationEventSubscriber;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Wraps an instance of {@link Persistor} allowing its behaviour to be composed.
 *
 * @param <CN> The type which the persistor uses to interact with the data store.
 * @param <TX> The transaction type.
 */
@Beta
public class PersistorWrapper<CN, TX extends BaseTransaction<CN>>
    implements Persistor<CN, TX>, InitializationEventSubscriber {

  private final Persistor<CN, TX> delegate;

  public PersistorWrapper(Persistor<CN, TX> delegate) {
    this.delegate = delegate;
  }

  @Override
  public void migrate(BaseTransactionManager<CN, ? extends TX> tm) {
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
  public CompletableFuture<Boolean> unblock(TX tx, String entryId) {
    return delegate.unblock(tx, entryId);
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

  @Override
  public void onRegisterInitializationEvents(InitializationEventBus eventBus) {
    if (delegate instanceof InitializationEventSubscriber) {
      ((InitializationEventSubscriber) delegate).onRegisterInitializationEvents(eventBus);
    }
  }
}
