package com.gruelbox.transactionoutbox;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
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
  public void save(Transaction tx, TransactionOutboxEntry entry) {
    // No-op
  }

  @Override
  public void delete(Transaction tx, TransactionOutboxEntry entry) {
    // No-op
  }

  @Override
  public void update(Transaction tx, TransactionOutboxEntry entry) {
    // No-op
  }

  @Override
  public boolean lock(Transaction tx, TransactionOutboxEntry entry) {
    return true;
  }

  @Override
  public boolean unblock(Transaction tx, String entryId) {
    return true;
  }

  @Override
  public List<TransactionOutboxEntry> selectBatch(Transaction tx, int batchSize, Instant now) {
    return List.of();
  }

  @Override
  public List<String> selectActiveTopics(Transaction tx) throws Exception {
    return List.of();
  }

  @Override
  public Optional<TransactionOutboxEntry> nextInTopic(Transaction tx, String topic)
      throws Exception {
    return Optional.empty();
  }

  @Override
  public int deleteProcessedAndExpired(Transaction tx, int batchSize, Instant now) {
    return 0;
  }

  @Override
  public void clear(Transaction tx) {}

  @Override
  public boolean checkConnection(Transaction tx) {
    return true;
  }
}
