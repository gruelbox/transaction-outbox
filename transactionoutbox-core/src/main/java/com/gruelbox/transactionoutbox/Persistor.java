package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.DefaultPersistor.DefaultPersistorBuilder;
import java.util.List;

/**
 * Saves and loads {@link TransactionOutboxEntry}s. May make this public to allow modification at
 * some point.
 */
public interface Persistor {

  /**
   * Uses the default relational persistor.
   *
   * @param dialect The database dialect.
   * @return The persistor.
   */
  static DefaultPersistorBuilder forDialect(Dialect dialect) {
    return DefaultPersistor.builder().dialect(dialect);
  }

  /**
   * Upgrades the database tables used by the persistor to the latest version. Called on creation of
   * a {@link TransactionOutbox}.
   *
   * @param transactionManager The transactoin manager.
   */
  void migrate(TransactionManager transactionManager);

  /**
   * Saves a new {@link TransactionOutboxEntry}.
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

  void update(Transaction tx, TransactionOutboxEntry entry) throws Exception;

  boolean lock(Transaction tx, TransactionOutboxEntry entry) throws Exception;

  List<TransactionOutboxEntry> selectBatch(Transaction tx, int batchSize) throws Exception;
}
