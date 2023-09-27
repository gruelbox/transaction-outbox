package com.gruelbox.transactionoutbox;

import java.time.Instant;
import java.util.List;

/**
 * Saves and loads {@link TransactionOutboxEntry}s. For most use cases, just use {@link
 * DefaultPersistor}. It is parameterisable and designed for extension, so can be easily modified.
 * Creating completely new implementations of {@link Persistor} should be reserved for cases where
 * the underlying data store is of a completely different nature entirely.
 */
public interface Persistor {

  /**
   * Uses the default relational persistor. Shortcut for: <code>
   * DefaultPersistor.builder().dialect(dialect).build();</code>
   *
   * @param dialect The database dialect.
   * @return The persistor.
   */
  static DefaultPersistor forDialect(Dialect dialect) {
    return DefaultPersistor.builder().dialect(dialect).build();
  }

  /**
   * Upgrades any database schema used by the persistor to the latest version. Called on creation of
   * a {@link TransactionOutbox}.
   *
   * @param transactionManager The transactoin manager.
   */
  void migrate(TransactionManager transactionManager);

  /**
   * Saves a new {@link TransactionOutboxEntry}. Should throw {@link AlreadyScheduledException} if
   * the record already exists based on the {@code id} or {@code uniqueRequestId} (the latter of
   * which should not treat nulls as duplicates).
   *
   * @param tx The current {@link Transaction}.
   * @param entry The entry to save. All properties on the object should be saved recursively.
   * @throws Exception Any exception.
   */
  void save(Transaction tx, TransactionOutboxEntry entry) throws Exception;

  /**
   * Deletes a {@link TransactionOutboxEntry}.
   *
   * <p>A record should only be deleted if <em>both</em> the {@code id} and {@code version} on the
   * database match that on the object. If no such record is found, {@link OptimisticLockException}
   * should be thrown.
   *
   * @param tx The current {@link Transaction}.
   * @param entry The entry to be deleted.
   * @throws OptimisticLockException If no such record is found.
   * @throws Exception Any other exception.
   */
  void delete(Transaction tx, TransactionOutboxEntry entry) throws Exception;

  /**
   * Modifies an existing {@link TransactionOutboxEntry}. Performs an optimistic lock check on any
   * existing record via a compare-and-swap operation and throws {@link OptimisticLockException} if
   * the lock is failed. {@link TransactionOutboxEntry#setVersion(int)} is called before returning
   * containing the new version of the entry.
   *
   * @param tx The current {@link Transaction}.
   * @param entry The entry to be updated.
   * @throws OptimisticLockException If no record with same id and version is found.
   * @throws Exception Any other exception.
   */
  void update(Transaction tx, TransactionOutboxEntry entry) throws Exception;

  /**
   * Attempts to pessimistically lock an existing {@link TransactionOutboxEntry}.
   *
   * @param tx The current {@link Transaction}.
   * @param entry The entry to be locked
   * @return true if the lock was successful.
   * @throws OptimisticLockException If no record with same id and version is found.
   * @throws Exception Any other exception.
   */
  boolean lock(Transaction tx, TransactionOutboxEntry entry) throws Exception;

  /**
   * Clears the blocked flag and resets the attempt count to zero.
   *
   * @param tx The current {@link Transaction}.
   * @param entryId The entry id.
   * @return true if the update was successful. This will be false if the record was no longer
   *     blocked or didn't exist anymore.
   * @throws Exception Any other exception.
   */
  boolean unblock(Transaction tx, String entryId) throws Exception;

  /**
   * Clears the blocked flag and resets the attempt count to zero for all blocked entries.
   *
   * @param tx The current {@link Transaction}.
   * @return unblocked entries count
   * @throws Exception Any other exception.
   */
  int unblockAll(Transaction tx) throws Exception;

  /**
   * Selects up to a specified maximum number of blocked records with pagination.
   *
   * @param tx The current {@link Transaction}.
   * @param page the page to get
   * @param batchSize The number of records to select.
   * @return The records.
   * @throws Exception Any exception.
   */
  List<TransactionOutboxEntry> selectBlocked(Transaction tx, int page, int batchSize)
      throws Exception;

  /**
   * Selects up to a specified maximum number of non-blocked records which have passed their {@link
   * TransactionOutboxEntry#getNextAttemptTime()}. Until a subsequent call to {@link
   * #lock(Transaction, TransactionOutboxEntry)}, these records may be selected by another instance
   * for processing.
   *
   * @param tx The current {@link Transaction}.
   * @param batchSize The number of records to select.
   * @param now The time to use when selecting records.
   * @return The records.
   * @throws Exception Any exception.
   */
  List<TransactionOutboxEntry> selectBatch(Transaction tx, int batchSize, Instant now)
      throws Exception;

  int deleteProcessedAndExpired(Transaction tx, int batchSize, Instant now) throws Exception;
}
