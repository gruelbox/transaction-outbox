package com.gruelbox.transactionoutbox;

import java.util.List;

/**
 * Saves and loads {@link TransactionOutboxEntry}s. May make this public to allow modification at
 * some point.
 */
@SuppressWarnings("WeakerAccess")
interface Persistor {

  /**
   * Saves a new {@link TransactionOutboxEntry}.
   *
   * @param tx The {@link TransactionManager}. {@link TransactionManager#getActiveConnection()} will
   *     be used to participate in an ongoing transaction, which must exist. No new transactions
   *     should be created.
   * @param entry The entry to save. All properties on the object should be saved recursively.
   * @throws Exception Any exception.
   */
  void save(TransactionManager tx, TransactionOutboxEntry entry) throws Exception;

  /**
   * Deletes a {@link TransactionOutboxEntry}.
   *
   * <p>A record should only be deleted if <em>both</em> the {@code id} and {@code version} on the
   * database match that on the object. If no such record is found, {@link OptimisticLockException}
   * should be thrown.
   *
   * @param tx The {@link TransactionManager}. {@link TransactionManager#getActiveConnection()} will
   *     be used to participate in an ongoing transaction, which must exist. No new transactions
   *     should be created.
   * @param entry The entry to be deleted.
   * @throws OptimisticLockException If no such record is found.
   * @throws Exception Any other exception.
   */
  void delete(TransactionManager tx, TransactionOutboxEntry entry) throws Exception;

  void update(TransactionManager tx, TransactionOutboxEntry entry) throws Exception;

  boolean lock(TransactionManager tx, TransactionOutboxEntry entry) throws Exception;

  boolean lockSkippingLocks(TransactionManager tx, TransactionOutboxEntry entry) throws Exception;

  List<TransactionOutboxEntry> selectBatch(TransactionManager tx, int batchSize) throws Exception;

  List<TransactionOutboxEntry> selectBatchSkippingLocksForUpdate(
      TransactionManager tx, int batchSize) throws Exception;
}
