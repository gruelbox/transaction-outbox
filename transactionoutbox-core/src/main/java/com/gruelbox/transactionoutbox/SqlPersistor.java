package com.gruelbox.transactionoutbox;

import static java.util.concurrent.CompletableFuture.failedFuture;

import java.io.StringReader;
import java.io.StringWriter;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

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
    this.clearSql = handler.preprocessSql(dialect, "DELETE FROM " + this.tableName);
  }

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
                "CREATE TABLE " + tableName + " (\n"
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
                "ALTER TABLE " + tableName + " ADD COLUMN uniqueRequestId VARCHAR(100) NULL UNIQUE"),
            new Migration(
                3, "Add processed flag", "ALTER TABLE " + tableName + " ADD COLUMN processed BOOLEAN"),
            new Migration(
                4,
                "Add flush index",
                "CREATE INDEX IX_" + tableName + "_1 ON " + tableName + " (processed, blacklisted, nextAttemptTime)"));

    // TODO block multiple nodes doing this somehow in a way that allows blocking...
    runSimpleSqlBlocking(tm, "CREATE TABLE IF NOT EXISTS TXNO_VERSION (version INT)");
    Integer currentVersion =
        runSimpleQueryBlocking(
                tm, "SELECT version FROM TXNO_VERSION FOR UPDATE", row -> row.get(0, Integer.class))
            .stream()
            .findFirst()
            .orElse(0);
    allMigrations.stream()
        .filter(it -> it.version > currentVersion)
        .forEach(
            migration -> {
              log.info("Running migration: {}", migration.name);
              runSimpleSqlBlocking(tm, migration.sql);
              if (runSimpleSqlBlocking(tm, "UPDATE TXNO_VERSION SET version = " + migration.version)
                  == 0) {
                runSimpleSqlBlocking(
                    tm, "INSERT INTO TXNO_VERSION VALUES (" + migration.version + ")");
              }
            });
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
                return null;
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
      return handler.statement(
          tx,
          dialect,
          selectBatchSql,
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
    return handler.statement(tx, dialect, clearSql, false, Binder::execute);
  }

  @Beta
  public interface Handler<CN, TX extends Transaction<CN, ?>> {
    default String preprocessSql(Dialect dialect, String sql) {
      return sql;
    }

    <T> T statement(
        TX tx, Dialect dialect, String sql, boolean batchable, Function<Binder, T> binding);

    default void interceptNew(Dialect dialect, TransactionOutboxEntry entry) {
      // No-op
    }

    void handleSaveException(TransactionOutboxEntry entry, Throwable t);
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
      return new SqlPersistor<CN, TX>(
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

  private <T> List<T> runSimpleQueryBlocking(
      TransactionManager<CN, ?, ? extends TX> transactionManager,
      String sql,
      Function<ResultRow, T> mapper) {
    return transactionManager
        .transactionally(
            tx ->
                handler.statement(
                    tx, dialect, sql, false, binder -> binder.executeQuery(1, mapper)))
        .join();
  }

  private int runSimpleSqlBlocking(
      TransactionManager<CN, ?, ? extends TX> transactionManager, String sql) {
    return transactionManager
        .transactionally(tx -> handler.statement(tx, dialect, sql, false, Binder::execute))
        .join();
  }

  @AllArgsConstructor
  private static final class Migration {
    private final int version;
    private final String name;
    private final String sql;
  }
}