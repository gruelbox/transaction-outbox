package com.gruelbox.transactionoutbox;

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
  static DefaultPersistor forDialect(Dialect dialect) {
    return DefaultPersistor.builder().dialect(dialect).build();
  }

  /**
   * Uses the default relational persistor with an alternative serializer.
   *
   * <p>An {@link Invocation} is normally serialised to the database as JSON, since methods could
   * take any number of arguments of a variety of parameter types. The default serializer has a
   * number of limitations. Only the following are supported currently:
   *
   * <ul>
   *   <li>Primitive types such as {@code int} or {@code double} or the boxed equivalents.
   *   <li>{@link String}
   *   <li>{@link java.util.Date}
   *   <li>The {@code java.time} classes:
   *       <ul>
   *         <li>{@link java.time.DayOfWeek}
   *         <li>{@link java.time.Duration}
   *         <li>{@link java.time.Instant}
   *         <li>{@link java.time.LocalDate}
   *         <li>{@link java.time.LocalDateTime}
   *         <li>{@link java.time.Month}
   *         <li>{@link java.time.MonthDay}
   *         <li>{@link java.time.Period}
   *         <li>{@link java.time.Year}
   *         <li>{@link java.time.YearMonth}
   *         <li>{@link java.time.ZoneOffset}
   *         <li>{@link java.time.DayOfWeek}
   *         <li>{@link java.time.temporal.ChronoUnit}
   *       </ul>
   *   <li>Arrays specifically typed to one of the above types.
   * </ul>
   *
   * <p>If any of these limitations restrict your application, or if you don't need anything with
   * such general support and want a more specific, faster implementation, you can provide your own
   * {@link InvocationSerializer} here.
   *
   * @param dialect The database dialect.
   * @param serializer The serializer.
   * @return The persistor.
   */
  static DefaultPersistor forDialect(Dialect dialect, InvocationSerializer serializer) {
    return DefaultPersistor.builder().dialect(dialect).serializer(serializer).build();
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
