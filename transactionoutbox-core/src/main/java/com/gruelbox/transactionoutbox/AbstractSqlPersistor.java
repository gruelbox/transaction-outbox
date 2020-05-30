package com.gruelbox.transactionoutbox;

import static java.util.concurrent.CompletableFuture.failedFuture;

import java.io.StringReader;
import java.io.StringWriter;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import javax.validation.constraints.NotNull;
import lombok.Builder;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

@Beta
@SuperBuilder
@Slf4j
public abstract class AbstractSqlPersistor<CN, TX extends Transaction<CN, ?>>
    implements Persistor<CN, TX> {

  /**
   * @param writeLockTimeoutSeconds How many seconds to wait before timing out on obtaining a write
   *     lock. There's no point making this long; it's always better to just back off as quickly as
   *     possible and try another record. Generally these lock timeouts only kick in if {@link
   *     Dialect#isSupportsSkipLock()} is false.
   */
  @SuppressWarnings("JavaDoc")
  @Builder.Default
  @NotNull
  private final int writeLockTimeoutSeconds = 2;

  /** @param dialect The database dialect to use. Required. */
  @SuppressWarnings("JavaDoc")
  @NotNull
  protected final Dialect dialect;

  /** @param tableName The database table name. The default is {@code TXNO_OUTBOX}. */
  @SuppressWarnings("JavaDoc")
  @Builder.Default
  @NotNull
  private final String tableName = "TXNO_OUTBOX";

  /**
   * @param migrate Set to false to disable automatic database migrations. This may be preferred if
   *     the default migration behaviour interferes with your existing toolset, and you prefer to
   *     manage the migrations explicitly (e.g. using FlyWay or Liquibase), or your do not give the
   *     application DDL permissions at runtime.
   */
  @SuppressWarnings("JavaDoc")
  @Builder.Default
  @NotNull
  private final boolean migrate = true;

  /**
   * @param serializer The serializer to use for {@link Invocation}s. See {@link
   *     InvocationSerializer} for more information. Defaults to {@link
   *     InvocationSerializer#createDefaultJsonSerializer()} with no whitelisted classes..
   */
  @SuppressWarnings("JavaDoc")
  @Builder.Default
  private final InvocationSerializer serializer =
      InvocationSerializer.createDefaultJsonSerializer();

  private final ConcurrentMap<StatementKey, String> sqlCache = new ConcurrentHashMap<>();

  @Override
  public CompletableFuture<Void> save(TX tx, TransactionOutboxEntry entry) {
    try {
      var writer = new StringWriter();
      serializer.serializeInvocation(entry.getInvocation(), writer);
      String sql =
          sqlCache.computeIfAbsent(
              StatementKey.SAVE,
              k ->
                  preParse(
                      "INSERT INTO "
                          + tableName
                          + " (id, uniqueRequestId, invocation, nextAttemptTime, attempts, blacklisted, processed, version) "
                          + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"));
      return statement(
              tx,
              sql,
              entry.getUniqueRequestId() == null,
              binder ->
                  binder
                      .bind(0, entry.getId())
                      .bind(1, entry.getUniqueRequestId())
                      .bind(2, writer.toString())
                      .bind(3, entry.getNextAttemptTime())
                      .bind(4, entry.getAttempts())
                      .bind(5, entry.isBlacklisted())
                      .bind(6, entry.isProcessed())
                      .bind(7, entry.getVersion())
                      .execute())
          .thenCompose(Result::rowsUpdated)
          .thenApply(
              rows -> {
                if (rows != 1) {
                  throw new RuntimeException("Failed to insert record");
                }
                return null;
              });
    } catch (Exception e) {
      return failedFuture(e);
    }
  }

  @Override
  public final CompletableFuture<Void> delete(TX tx, TransactionOutboxEntry entry) {
    try {
      String sql =
          sqlCache.computeIfAbsent(
              StatementKey.DELETE,
              k -> preParse("DELETE FROM " + tableName + " WHERE id = ? and version = ?"));
      return statement(
              tx,
              sql,
              false,
              binder -> binder.bind(0, entry.getId()).bind(1, entry.getVersion()).execute())
          .thenCompose(Result::rowsUpdated)
          .thenApply(
              rows -> {
                if (rows != 1) {
                  throw new OptimisticLockException();
                } else {
                  log.debug("Deleted {}", entry.description());
                  return null;
                }
              });
    } catch (Exception e) {
      return failedFuture(e);
    }
  }

  @Override
  public final CompletableFuture<Void> update(TX tx, TransactionOutboxEntry entry) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final CompletableFuture<Boolean> lock(TX tx, TransactionOutboxEntry entry) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final CompletableFuture<Boolean> whitelist(TX tx, String entryId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final CompletableFuture<List<TransactionOutboxEntry>> selectBatch(
      TX tx, int batchSize, Instant now) {
    try {
      String forUpdate = dialect.isSupportsSkipLock() ? " FOR UPDATE SKIP LOCKED" : "";
      String sql =
          sqlCache.computeIfAbsent(
              StatementKey.SELECT_BATCH,
              k ->
                  preParse(
                      "SELECT id, uniqueRequestId, invocation, nextAttemptTime, attempts, blacklisted, processed, version "
                          + "FROM "
                          + tableName
                          + " WHERE nextAttemptTime < ?"
                          + " AND blacklisted = false AND processed = false LIMIT ?"
                          + forUpdate));
      return statement(
          tx,
          sql,
          false,
          binder ->
              binder
                  .bind(0, now)
                  .bind(1, batchSize)
                  .executeQuery(
                      batchSize,
                      rs ->
                          TransactionOutboxEntry.builder()
                              .id(rs.get(0, String.class))
                              .uniqueRequestId(rs.get(1, String.class))
                              .invocation(
                                  serializer.deserializeInvocation(
                                      new StringReader(rs.get(2, String.class))))
                              .nextAttemptTime(rs.get(3, Instant.class))
                              .attempts(rs.get(4, Integer.class))
                              .blacklisted(rs.get(5, Boolean.class))
                              .processed(rs.get(6, Boolean.class))
                              .version(rs.get(7, Integer.class))
                              .build()));
    } catch (Exception e) {
      return failedFuture(e);
    }
  }

  @Override
  public final CompletableFuture<Integer> deleteProcessedAndExpired(
      TX tx, int batchSize, Instant now) {
    throw new UnsupportedOperationException();
  }

  protected abstract <T> T statement(
      TX tx, String sql, boolean batchable, Function<Binder, T> binding);

  protected String preParse(String sql) {
    return sql;
  }

  // For testing. Assumed low volume.
  public final CompletableFuture<Void> clear(TX tx) {
    return statement(tx, "DELETE FROM " + tableName, false, Binder::execute).thenApply(__ -> null);
  }

  enum StatementKey {
    SAVE,
    SELECT_BATCH,
    DELETE
  }

  protected interface Result {
    CompletableFuture<Integer> rowsUpdated();
  }

  protected interface ResultRow {
    <T> T get(int index, Class<T> type);
  }

  protected interface Binder {
    Binder bind(int index, Object value);

    CompletableFuture<Result> execute();

    <T> CompletableFuture<List<T>> executeQuery(
        int expectedRowCount, Function<ResultRow, T> rowMapper);
  }

  //  void deleteBlocking(JdbcTransaction<?> tx, TransactionOutboxEntry entry) throws Exception {
  //    try (PreparedStatement stmt =
  //        // language=MySQL
  //        tx.connection()
  //            .prepareStatement("DELETE FROM " + tableName + " WHERE id = ? and version = ?")) {
  //      stmt.setString(1, entry.getId());
  //      stmt.setInt(2, entry.getVersion());
  //      if (stmt.executeUpdate() != 1) {
  //        throw new OptimisticLockException();
  //      }
  //      log.debug("Deleted {}", entry.description());
  //    }
  //  }
  //
  //  void updateBlocking(JdbcTransaction<?> tx, TransactionOutboxEntry entry) throws Exception {
  //    try (PreparedStatement stmt =
  //        tx.connection()
  //            .prepareStatement(
  //                // language=MySQL
  //                "UPDATE "
  //                    + tableName
  //                    + " "
  //                    + "SET nextAttemptTime = ?, attempts = ?, blacklisted = ?, processed = ?,
  // version = ? "
  //                    + "WHERE id = ? and version = ?")) {
  //      stmt.setTimestamp(1, Timestamp.from(entry.getNextAttemptTime()));
  //      stmt.setInt(2, entry.getAttempts());
  //      stmt.setBoolean(3, entry.isBlacklisted());
  //      stmt.setBoolean(4, entry.isProcessed());
  //      stmt.setInt(5, entry.getVersion() + 1);
  //      stmt.setString(6, entry.getId());
  //      stmt.setInt(7, entry.getVersion());
  //      if (stmt.executeUpdate() != 1) {
  //        throw new OptimisticLockException();
  //      }
  //      entry.setVersion(entry.getVersion() + 1);
  //      log.debug("Updated {}", entry.description());
  //    }
  //  }
  //
  //  boolean lockBlocking(JdbcTransaction<?> tx, TransactionOutboxEntry entry) throws Exception {
  //    try (PreparedStatement stmt =
  //        tx.connection()
  //            .prepareStatement(
  //                dialect.isSupportsSkipLock()
  //                    // language=MySQL
  //                    ? "SELECT id FROM "
  //                    + tableName
  //                    + " WHERE id = ? AND version = ? FOR UPDATE SKIP LOCKED"
  //                    // language=MySQL
  //                    : "SELECT id FROM " + tableName + " WHERE id = ? AND version = ? FOR
  // UPDATE")) {
  //      stmt.setString(1, entry.getId());
  //      stmt.setInt(2, entry.getVersion());
  //      stmt.setQueryTimeout(writeLockTimeoutSeconds);
  //      return gotRecord(entry, stmt);
  //    }
  //  }
  //
  //  boolean whitelistBlocking(JdbcTransaction<?> tx, String entryId) throws Exception {
  //    PreparedStatement stmt =
  //        tx.prepareBatchStatement(
  //            "UPDATE "
  //                + tableName
  //                + " SET attempts = 0, blacklisted = false "
  //                + "WHERE blacklisted = true AND processed = false AND id = ?");
  //    stmt.setString(1, entryId);
  //    stmt.setQueryTimeout(writeLockTimeoutSeconds);
  //    return stmt.executeUpdate() != 0;
  //  }
  //
  //  List<TransactionOutboxEntry> selectBatchBlocking(
  //      JdbcTransaction<?> tx, int batchSize, Instant now) throws Exception {
  //    String forUpdate = dialect.isSupportsSkipLock() ? " FOR UPDATE SKIP LOCKED" : "";
  //    try (PreparedStatement stmt =
  //        tx.connection()
  //            .prepareStatement(
  //                // language=MySQL
  //                "SELECT id, uniqueRequestId, invocation, nextAttemptTime, attempts, blacklisted,
  // processed, version FROM "
  //                    + tableName
  //                    + " WHERE nextAttemptTime < ? AND blacklisted = false AND processed = false
  // LIMIT ?"
  //                    + forUpdate)) {
  //      stmt.setTimestamp(1, Timestamp.from(now));
  //      stmt.setInt(2, batchSize);
  //      return gatherResults(batchSize, stmt);
  //    }
  //  }
  //
  //  private int deleteProcessedAndExpiredInner(JdbcTransaction<?> tx, int batchSize, Instant now)
  //      throws Exception {
  //    try (PreparedStatement stmt =
  //        tx.connection()
  //            .prepareStatement(dialect.getDeleteExpired().replace("{{table}}", tableName))) {
  //      stmt.setTimestamp(1, Timestamp.from(now));
  //      stmt.setInt(2, batchSize);
  //      return stmt.executeUpdate();
  //    }
  //  }
}
