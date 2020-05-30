package com.gruelbox.transactionoutbox;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Saves and loads {@link TransactionOutboxEntry}s. Intentially non-blocking API which can be
 * implemented in a blocking manner for intrinsically blocking datastore APIs such as JDBC.
 *
 * @param <CN> The type which the associated {@link Persistor} implementation will use to interact
 *     with the data store.
 * @param <TX> The transaction type
 */
public interface Persistor<CN, TX extends Transaction<CN, ?>> {

  /**
   * Upgrades any database schema used by the persistor to the latest version. Called on creation of
   * a {@link TransactionOutbox}.
   *
   * @param transactionManager The transaction manager.
   */
  void migrate(TransactionManager<CN, ?, ? extends TX> transactionManager);

  /**
   * Saves a new {@link TransactionOutboxEntry}. Should emit {@link AlreadyScheduledException} if
   * the record already exists based on the {@code id} or {@code uniqueRequestId} (the latter of
   * which should not treat nulls as duplicates).
   *
   * @param tx The current {@link Transaction}.
   * @param entry The entry to save. All properties on the object should be saved recursively.
   */
  CompletableFuture<Void> save(TX tx, TransactionOutboxEntry entry);

  /**
   * Deletes a {@link TransactionOutboxEntry}.
   *
   * <p>A record should only be deleted if <em>both</em> the {@code id} and {@code version} on the
   * database match that on the object. If no such record is found, {@link OptimisticLockException}
   * should be emitted.
   *
   * @param tx The current {@link Transaction}.
   * @param entry The entry to be deleted.
   */
  CompletableFuture<Void> delete(TX tx, TransactionOutboxEntry entry);

  /**
   * Modifies an existing {@link TransactionOutboxEntry}. Performs an optimistic lock check on any
   * existing record via a compare-and-swap operation and emits {@link OptimisticLockException} if
   * the lock is failed. {@link TransactionOutboxEntry#setVersion(int)} is called before returning
   * containing the new version of the entry.
   *
   * @param tx The current {@link Transaction}.
   * @param entry The entry to be updated.
   */
  CompletableFuture<Void> update(TX tx, TransactionOutboxEntry entry);

  /**
   * Attempts to pessimistically lock an existing {@link TransactionOutboxEntry}. Emits {@link
   * OptimisticLockException} if no record with same id and version is found.
   *
   * @param tx The current {@link Transaction}.
   * @param entry The entry to be locked
   * @return true if the lock was successful.
   */
  CompletableFuture<Boolean> lock(TX tx, TransactionOutboxEntry entry);

  /**
   * Clears the blacklisted flag and resets the attempt count to zero.
   *
   * @param tx The current {@link Transaction}.
   * @param entryId The entry id.
   * @return true if the update was successful. This will be false if the record was no longer
   *     blacklisted or didn't exist anymore.
   */
  CompletableFuture<Boolean> whitelist(TX tx, String entryId);

  /**
   * Selects up to a specified maximum number of non-blacklisted records which have passed their
   * {@link TransactionOutboxEntry#getNextAttemptTime()}. Until a subsequent call to {@link
   * #lock(Transaction, TransactionOutboxEntry)}, these records may be selected by another instance
   * for processing.
   *
   * @param tx The current {@link Transaction}.
   * @param batchSize The number of records to select.
   * @param now The time to use when selecting records.
   * @return The records.
   */
  CompletableFuture<List<TransactionOutboxEntry>> selectBatch(TX tx, int batchSize, Instant now);

  /**
   * Cleans up records which have been marked as processed but not deleted, and which have passed
   * their next process date/time.
   *
   * @param tx The current {@link Transaction}.
   * @param batchSize The number of records to select.
   * @param now The time to use when selecting records.
   * @return The number of records affected.
   */
  CompletableFuture<Integer> deleteProcessedAndExpired(TX tx, int batchSize, Instant now);
}
