package com.gruelbox.transactionoutbox;

import lombok.Builder;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Stub implementation of {@link Persistor}.
 */
@Builder
public class StubPersistor implements Persistor {

    StubPersistor() {
    }

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
    public Optional<TransactionOutboxEntry> findFirstOfGroup(Transaction tx) {
        return Optional.empty();
    }

    @Override
    public int deleteProcessedAndExpired(Transaction tx, int batchSize, Instant now) {
        return 0;
    }

    @Override
    public Optional<TransactionOutboxEntry> findByGroupIdBeforeCreatedAt(Transaction tx, String groupId, Instant createdAt) throws Exception {
        return Optional.empty();
    }
}
