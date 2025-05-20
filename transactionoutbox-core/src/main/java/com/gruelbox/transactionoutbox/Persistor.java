package com.gruelbox.transactionoutbox;

import java.time.Instant;
import java.util.Collection;
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
   * Modifies a batch of existing {@link TransactionOutboxEntry}s. Performs an optimistic lock check
   * on any existing record via a compare-and-swap operation and throws {@link
   * OptimisticLockException} if the lock is failed. {@link TransactionOutboxEntry#setVersion(int)}
   * is called before returning containing the new version of the entry.
   *
   * @param tx The current {@link Transaction}.
   * @param entries The entries to be updated.
   * @throws OptimisticLockException If no record with same id and version is found.
   * @throws Exception Any other exception.
   */
  void updateBatch(Transaction tx, List<TransactionOutboxEntry> entries) throws Exception;

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
   * Attempts to pessimistically lock all the entries in a batch using a single SQL statement where possible.
   * This is used for efficient batch processing.
   *
   * @param tx The current {@link Transaction}.
   * @param entries The list of entries to be locked.
   * @return true if all entries were successfully locked, false otherwise.
   * @throws Exception Any exception.
   */
  boolean lockBatch(Transaction tx, List<TransactionOutboxEntry> entries) throws Exception;

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

  /**
   * Selects the next items in all the open topics as a batch for processing. Does not lock.
   *
   * @param tx The current {@link Transaction}.
   * @param batchSize The maximum number of records to select.
   * @param now The time to use when selecting records.
   * @return The records.
   * @throws Exception Any exception.
   */
  Collection<TransactionOutboxEntry> selectNextInTopics(Transaction tx, int batchSize, Instant now)
      throws Exception;

  /**
   * Selects the next items in all selected topics as a batch for processing. Does not lock.
   *
   * @param tx The current {@link Transaction}.
   * @param topicNames The topics to select records from.
   * @param batchSize The maximum number of records to select.
   * @param now The time to use when selecting records.
   * @return The records.
   * @throws Exception Any exception.
   */
  Collection<TransactionOutboxEntry> selectNextInSelectedTopics(
      Transaction tx, List<String> topicNames, int batchSize, Instant now) throws Exception;

  /**
   * Selects the next batch of entries in topics, maintaining order within each topic. This method
   * is used for ordered batch processing and returns multiple entries per topic up to the batch
   * size limit.
   *
   * @param tx The current transaction
   * @param batchSize The maximum number of entries to return per topic
   * @param now The current time
   * @return A collection of entries ordered by topic and sequence
   * @throws Exception If an error occurs during selection
   */
  Collection<TransactionOutboxEntry> selectNextBatchInTopics(
      Transaction tx, int batchSize, Instant now) throws Exception;

  /**
   * Deletes records which have processed and passed their expiry time, in specified batch sizes.
   *
   * @param tx The current {@link Transaction}.
   * @param batchSize The maximum number of records to select.
   * @param now The time to use when selecting records.
   * @return The number of records deleted.
   * @throws Exception Any exception.
   */
  int deleteProcessedAndExpired(Transaction tx, int batchSize, Instant now) throws Exception;

  /**
   * Checks the connection status of a transaction.
   *
   * @param tx The current {@link Transaction}.
   * @return true if connected and working.
   */
  boolean checkConnection(Transaction tx) throws Exception;

  /**
   * Clears the database. For testing only.
   *
   * @param tx The current {@link Transaction}.
   */
  void clear(Transaction tx) throws Exception;
}
