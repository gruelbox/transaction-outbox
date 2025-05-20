package com.gruelbox.transactionoutbox;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
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
  public void updateBatch(Transaction tx, List<TransactionOutboxEntry> entries) throws Exception {
    // No-op
  }

  @Override
  public boolean lock(Transaction tx, TransactionOutboxEntry entry) {
    return true;
  }

  @Override
  public boolean lockBatch(Transaction tx, List<TransactionOutboxEntry> entries) {
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
  public Collection<TransactionOutboxEntry> selectNextInTopics(
      Transaction tx, int flushBatchSize, Instant now) {
    return List.of();
  }

  @Override
  public Collection<TransactionOutboxEntry> selectNextBatchInTopics(
      Transaction tx, int batchSize, Instant now) throws Exception {
    return List.of();
  }

  @Override
  public Collection<TransactionOutboxEntry> selectNextInSelectedTopics(
      Transaction tx, List<String> topicNames, int batchSize, Instant now) throws Exception {
    return List.of();
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
