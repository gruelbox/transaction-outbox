package com.gruelbox.transactionoutbox;

import static com.ea.async.Async.*;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

import java.io.StringReader;
import java.io.StringWriter;
import java.sql.SQLTimeoutException;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * Provides support for persistors using a SQL database, regardless of the *DBC API in use.
 *
 * <p>Not intended for direct use.
 *
 * @param <CN> The type that the persistor uses to interact with an active connection.
 * @param <TX> The transaction type.
 */
@Beta
@Slf4j
public final class SqlPersistor<CN, TX extends Transaction<CN, ?>> implements Persistor<CN, TX> {

  private final int writeLockTimeoutSeconds;
  private final Dialect dialect;
  private final String tableName;
  private final boolean migrate;
  private final InvocationSerializer serializer;
  private final Handler<CN, TX> handler;

  private final String insertSql;
  private final String selectBatchSql;
  private final String deleteSql;
  private final String updateSql;
  private final String lockSql;
  private final String clearSql;

  private SqlPersistor(
      Integer writeLockTimeoutSeconds,
      Dialect dialect,
      String tableName,
      Boolean migrate,
      InvocationSerializer serializer,
      Handler<CN, TX> handler) {
    this.writeLockTimeoutSeconds = writeLockTimeoutSeconds == null ? 2 : writeLockTimeoutSeconds;
    this.dialect = Objects.requireNonNull(dialect);
    this.tableName = tableName == null ? "TXNO_OUTBOX" : tableName;
    this.migrate = migrate == null ? true : migrate;
    this.serializer =
        Utils.firstNonNull(serializer, InvocationSerializer::createDefaultJsonSerializer);
    this.handler = Objects.requireNonNull(handler);
    this.insertSql =
        handler.preprocessSql(
            dialect,
            "INSERT INTO "
                + this.tableName
                + " (id, uniqueRequestId, invocation, nextAttemptTime, attempts, blacklisted, processed, version) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
    this.selectBatchSql =
        handler.preprocessSql(
            dialect,
            "SELECT id, uniqueRequestId, invocation, nextAttemptTime, attempts, blacklisted, processed, version "
                + "FROM "
                + this.tableName
                + " WHERE nextAttemptTime < ?"
                + " AND blacklisted = false AND processed = false LIMIT ?"
                + (dialect.isSupportsSkipLock() ? " FOR UPDATE SKIP LOCKED" : ""));
    this.deleteSql =
        handler.preprocessSql(
            dialect, "DELETE FROM " + this.tableName + " WHERE id = ? and version = ?");
    this.updateSql =
        handler.preprocessSql(
            dialect,
            "UPDATE "
                + this.tableName
                + " SET nextAttemptTime = ?, attempts = ?,"
                + " blacklisted = ?, processed = ?, version = ?"
                + " WHERE id = ? and version = ?");
    this.lockSql =
        handler.preprocessSql(
            dialect,
            "SELECT id FROM "
                + this.tableName
                + " WHERE id = ? AND version = ? FOR UPDATE"
                + (dialect.isSupportsSkipLock() ? " SKIP LOCKED" : ""));
    this.clearSql = handler.preprocessSql(dialect, "DELETE FROM " + this.tableName);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void migrate(TransactionManager<CN, ?, ? extends TX> tm) {
    if (!migrate) {
      return;
    }
    List<Migration> allMigrations =
        List.of(
            new Migration(
                1,
                "Create outbox table",
                "CREATE TABLE "
                    + tableName
                    + " (\n"
                    + "    id VARCHAR(36) PRIMARY KEY,\n"
                    + "    invocation TEXT,\n"
                    + "    nextAttemptTime TIMESTAMP(6),\n"
                    + "    attempts INT,\n"
                    + "    blacklisted BOOLEAN,\n"
                    + "    version INT\n"
                    + ")"),
            new Migration(
                2,
                "Add unique request id",
                "ALTER TABLE "
                    + tableName
                    + " ADD COLUMN uniqueRequestId VARCHAR(100) NULL UNIQUE"),
            new Migration(
                3,
                "Add processed flag",
                "ALTER TABLE " + tableName + " ADD COLUMN processed BOOLEAN"),
            new Migration(
                4,
                "Add flush index",
                "CREATE INDEX IX_"
                    + tableName
                    + "_1 "
                    + "ON "
                    + tableName
                    + " (processed, blacklisted, nextAttemptTime)"));
    tm.transactionally(
            tx -> {
              await(
                  executeUpdate(
                      tx,
                      "CREATE TABLE IF NOT EXISTS TXNO_VERSION AS SELECT CAST(0 AS "
                          + dialect.getIntegerCastType()
                          + ") AS version"));
              List<Integer> versionResult =
                  await(
                      executeQuery(
                          tx,
                          "SELECT version FROM TXNO_VERSION FOR UPDATE",
                          row -> row.get(0, Integer.class)));
              if (versionResult.size() != 1) {
                throw new IllegalStateException(
                    "Unexpected number of version records: " + versionResult.size());
              }
              int currentVersion = versionResult.get(0);
              Iterator<Migration> migrationIterator =
                  allMigrations.stream().filter(it -> it.version > currentVersion).iterator();
              while (migrationIterator.hasNext()) {
                var mig = migrationIterator.next();
                log.info("Running migration {}: {}", mig.version, mig.name);
                await(executeUpdate(tx, mig.sql));
                await(executeUpdate(tx, "UPDATE TXNO_VERSION SET version = " + mig.version));
              }
              return completedFuture(null);
            })
        .join();
    log.info("Migrations complete");
  }

  @Override
  public CompletableFuture<Void> save(TX tx, TransactionOutboxEntry entry) {
    try {
      handler.interceptNew(dialect, entry);
      var writer = new StringWriter();
      serializer.serializeInvocation(entry.getInvocation(), writer);
      return handler
          .statement(
              tx,
              dialect,
              insertSql,
              writeLockTimeoutSeconds,
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
          .exceptionally(
              e -> {
                handler.handleSaveException(entry, e);
                throw sneakyRethrow(e);
              })
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
      return handler
          .statement(
              tx,
              dialect,
              deleteSql,
              writeLockTimeoutSeconds,
              false,
              binder -> binder.bind(0, entry.getId()).bind(1, entry.getVersion()).execute())
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
    try {
      return handler
          .statement(
              tx,
              dialect,
              updateSql,
              writeLockTimeoutSeconds,
              false,
              binder ->
                  binder
                      .bind(0, entry.getNextAttemptTime())
                      .bind(1, entry.getAttempts())
                      .bind(2, entry.isBlacklisted())
                      .bind(3, entry.isProcessed())
                      .bind(4, entry.getVersion() + 1)
                      .bind(5, entry.getId())
                      .bind(6, entry.getVersion())
                      .execute())
          .thenApply(
              rows -> {
                if (rows != 1) {
                  throw new OptimisticLockException();
                } else {
                  log.debug("Updated {}", entry.description());
                  entry.setVersion(entry.getVersion() + 1);
                  return null;
                }
              });
    } catch (Exception e) {
      return failedFuture(e);
    }
  }

  @Override
  public final CompletableFuture<Boolean> lock(TX tx, TransactionOutboxEntry entry) {
    try {
      return handler
          .statement(
              tx,
              dialect,
              lockSql,
              writeLockTimeoutSeconds,
              false,
              binder ->
                  binder
                      .bind(0, entry.getId())
                      .bind(1, entry.getVersion())
                      .executeQuery(1, row -> 1))
          .exceptionally(
              e -> {
                try {
                  return handler.handleLockException(entry, e);
                } catch (Throwable t) {
                  throw sneakyRethrow(t);
                }
              })
          .thenApply(list ->!list.isEmpty());
    } catch (Exception e) {
      return failedFuture(e);
    }
  }

  @SneakyThrows
  private RuntimeException sneakyRethrow(Throwable t) {
    throw t;
  }

  @Override
  public final CompletableFuture<Boolean> whitelist(TX tx, String entryId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final CompletableFuture<List<TransactionOutboxEntry>> selectBatch(
      TX tx, int batchSize, Instant now) {
    try {
      return handler.statement(
          tx,
          dialect,
          selectBatchSql,
          0,
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

  @Override
  public final CompletableFuture<Integer> clear(TX tx) {
    return handler.statement(
        tx, dialect, clearSql, 0, false, Binder::execute);
  }

  @Beta
  public interface Handler<CN, TX extends Transaction<CN, ?>> {
    default String preprocessSql(Dialect dialect, String sql) {
      return sql;
    }

    <T> T statement(
        TX tx,
        Dialect dialect,
        String sql,
        int writeLockTimeoutSeconds,
        boolean batchable,
        Function<Binder, T> binding);

    default void interceptNew(Dialect dialect, TransactionOutboxEntry entry) {
      // No-op
    }

    void handleSaveException(TransactionOutboxEntry entry, Throwable t);

    List<Integer> handleLockException(TransactionOutboxEntry entry, Throwable t) throws Throwable;
  }

  @Beta
  public interface ResultRow {
    <T> T get(int index, Class<T> type);
  }

  @Beta
  public interface Binder {
    Binder bind(int index, Object value);

    CompletableFuture<Integer> execute();

    <T> CompletableFuture<List<T>> executeQuery(
        int expectedRowCount, Function<ResultRow, T> rowMapper);
  }

  @Beta
  public abstract static class SqlPersistorBuilder<
      CN, TX extends Transaction<CN, ?>, T extends SqlPersistorBuilder<CN, TX, T>> {
    private final Handler<CN, TX> statementProcessor;
    private Integer writeLockTimeoutSeconds = 2;
    private Dialect dialect;
    private String tableName;
    private Boolean migrate;
    private InvocationSerializer serializer;

    protected SqlPersistorBuilder(Handler<CN, TX> statementProcessor) {
      this.statementProcessor = statementProcessor;
    }

    /**
     * @param writeLockTimeoutSeconds How many seconds to wait before timing out on obtaining a
     *     write lock. There's no point making this long; it's always better to just back off as
     *     quickly as possible and try another record. Generally these lock timeouts only kick in if
     *     {@link Dialect#isSupportsSkipLock()} is false.
     */
    @SuppressWarnings("unchecked")
    public T writeLockTimeoutSeconds(Integer writeLockTimeoutSeconds) {
      this.writeLockTimeoutSeconds = writeLockTimeoutSeconds;
      return (T) this;
    }

    /** @param dialect The database dialect to use. Required. */
    @SuppressWarnings("unchecked")
    public T dialect(Dialect dialect) {
      this.dialect = dialect;
      return (T) this;
    }

    /** @param tableName The database table name. The default is {@code TXNO_OUTBOX}. */
    @SuppressWarnings("unchecked")
    public T tableName(String tableName) {
      this.tableName = tableName;
      return (T) this;
    }

    /**
     * @param migrate Set to false to disable automatic database migrations. This may be preferred
     *     if the default migration behaviour interferes with your existing toolset, and you prefer
     *     to manage the migrations explicitly (e.g. using FlyWay or Liquibase), or your do not give
     *     the application DDL permissions at runtime.
     */
    @SuppressWarnings("unchecked")
    public T migrate(Boolean migrate) {
      this.migrate = migrate;
      return (T) this;
    }

    /**
     * @param serializer The serializer to use for {@link Invocation}s. See {@link
     *     InvocationSerializer} for more information. Defaults to {@link
     *     InvocationSerializer#createDefaultJsonSerializer()} with no whitelisted classes..
     */
    @SuppressWarnings("unchecked")
    public T serializer(InvocationSerializer serializer) {
      this.serializer = serializer;
      return (T) this;
    }

    protected SqlPersistor<CN, TX> buildGeneric() {
      return new SqlPersistor<>(
          writeLockTimeoutSeconds, dialect, tableName, migrate, serializer, statementProcessor);
    }

    public String toString() {
      return "SqlPersistor.SqlPersistorBuilder(writeLockTimeoutSeconds="
          + this.writeLockTimeoutSeconds
          + ", dialect="
          + this.dialect
          + ", tableName="
          + this.tableName
          + ", migrate="
          + this.migrate
          + ", serializer="
          + this.serializer
          + ", statementProcessor="
          + this.statementProcessor
          + ")";
    }
  }

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

  private <T> CompletableFuture<List<T>> executeQuery(
      TX tx, String sql, Function<ResultRow, T> mapper) {
    return handler.statement(
        tx, dialect, sql, writeLockTimeoutSeconds, false, binder -> binder.executeQuery(1, mapper));
  }

  private CompletableFuture<Integer> executeUpdate(TX tx, String sql) {
    return handler.statement(tx, dialect, sql, writeLockTimeoutSeconds, false, Binder::execute);
  }

  @AllArgsConstructor
  private static final class Migration {
    private final int version;
    private final String name;
    private final String sql;
  }
}
