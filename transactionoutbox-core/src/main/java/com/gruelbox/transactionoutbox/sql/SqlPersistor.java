package com.gruelbox.transactionoutbox.sql;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

import com.gruelbox.transactionoutbox.Beta;
import com.gruelbox.transactionoutbox.Invocation;
import com.gruelbox.transactionoutbox.InvocationSerializer;
import com.gruelbox.transactionoutbox.LockException;
import com.gruelbox.transactionoutbox.OptimisticLockException;
import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.Utils;
import com.gruelbox.transactionoutbox.spi.BaseTransaction;
import com.gruelbox.transactionoutbox.spi.BaseTransactionManager;
import com.gruelbox.transactionoutbox.spi.InitializationEventBus;
import com.gruelbox.transactionoutbox.spi.InitializationEventSubscriber;
import java.io.StringReader;
import java.io.StringWriter;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
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
public final class SqlPersistor<CN, TX extends BaseTransaction<CN>>
    implements Persistor<CN, TX>, InitializationEventSubscriber {

  private final int writeLockTimeoutSeconds;
  private final Dialect dialect;
  private final String tableName;
  private final boolean migrate;
  private final InvocationSerializer serializer;
  private final SqlApi<CN, TX> sqlApi;

  private final String insertSql;
  private final String selectBatchSql;
  private final String deleteExpiredSql;
  private final String deleteSql;
  private final String updateSql;
  private final String lockSql;
  private final String whitelistSql;
  private final String clearSql;

  private SqlPersistor(
      Integer writeLockTimeoutSeconds,
      Dialect dialect,
      String tableName,
      Boolean migrate,
      InvocationSerializer serializer,
      SqlApi<CN, TX> sqlApi) {
    this.writeLockTimeoutSeconds = writeLockTimeoutSeconds == null ? 2 : writeLockTimeoutSeconds;
    this.dialect = Objects.requireNonNull(dialect);
    this.tableName = tableName == null ? "TXNO_OUTBOX" : tableName;
    this.migrate = migrate == null ? true : migrate;
    this.serializer =
        Utils.firstNonNull(serializer, InvocationSerializer::createDefaultJsonSerializer);
    this.sqlApi = Objects.requireNonNull(sqlApi);
    this.insertSql =
        mapToNative(
            "INSERT INTO "
                + this.tableName
                + " (id, uniqueRequestId, invocation, nextAttemptTime, attempts, blacklisted, processed, version) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
    this.selectBatchSql =
        mapToNative(
            "SELECT id, uniqueRequestId, invocation, nextAttemptTime, attempts, blacklisted, processed, version "
                + "FROM "
                + this.tableName
                + " WHERE nextAttemptTime < ?"
                + " AND blacklisted = false AND processed = false LIMIT ?"
                + (dialect.isSupportsSkipLock() ? " FOR UPDATE SKIP LOCKED" : ""));
    this.deleteExpiredSql =
        mapToNative(dialect.getDeleteExpired().replace("{{table}}", this.tableName));
    this.deleteSql = mapToNative("DELETE FROM " + this.tableName + " WHERE id = ? and version = ?");
    this.updateSql =
        mapToNative(
            "UPDATE "
                + this.tableName
                + " SET nextAttemptTime = ?, attempts = ?,"
                + " blacklisted = ?, processed = ?, version = ?"
                + " WHERE id = ? and version = ?");
    this.lockSql =
        mapToNative(
            "SELECT id FROM "
                + this.tableName
                + " WHERE id = ? AND version = ? FOR UPDATE"
                + (dialect.isSupportsSkipLock() ? " SKIP LOCKED" : ""));
    this.whitelistSql =
        mapToNative(
            "UPDATE "
                + this.tableName
                + " SET attempts = 0, blacklisted = false, version = version + 1 "
                + "WHERE blacklisted = true AND processed = false AND id = ?");
    this.clearSql = dialect.mapStatementToNative("DELETE FROM " + this.tableName);
  }

  @Override
  public void migrate(BaseTransactionManager<CN, ? extends TX> tm) {
    if (!migrate) {
      return;
    }
    tm.transactionally(
            tx ->
                createOutboxTableIfNotExists(tx)
                    .thenCompose(__ -> fetchCurrentVersion(tx))
                    .thenCompose(
                        versionResult -> {
                          if (versionResult.size() != 1) {
                            return failedFuture(
                                new IllegalStateException(
                                    "Unexpected number of version records: "
                                        + versionResult.size()));
                          }
                          int currentVersion = versionResult.get(0);
                          log.info("Current schema version is {}", currentVersion);
                          return dialect
                              .migrations(tableName)
                              .filter(mig -> mig.getVersion() > currentVersion)
                              .peek(
                                  mig ->
                                      log.info(
                                          "Running migration {}: {}",
                                          mig.getVersion(),
                                          mig.getName()))
                              .map(
                                  mig ->
                                      executeUpdate(tx, mig.getSql())
                                          .thenCompose(__ -> updateVersion(tx, mig)))
                              .reduce(
                                  CompletableFuture.completedFuture(null),
                                  (f1, f2) -> f1.thenCompose(__ -> f2));
                        }))
        .join();
    log.info("Migrations complete");
  }

  @Override
  public CompletableFuture<Void> save(TX tx, TransactionOutboxEntry entry) {
    try {
      var writer = new StringWriter();
      serializer.serializeInvocation(entry.getInvocation(), writer);
      return sqlApi
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
                throw sneakyRethrow(sqlApi.mapSaveException(entry, e));
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
      return sqlApi
          .statement(
              tx,
              dialect,
              deleteSql,
              writeLockTimeoutSeconds,
              false,
              binder -> binder.bind(0, entry.getId()).bind(1, entry.getVersion()).execute())
          .thenCompose(
              rows -> {
                if (rows == 1) {
                  log.debug("Deleted {}", entry.description());
                  return completedFuture(null);
                } else if (rows == 0) {
                  return failedFuture(new OptimisticLockException());
                } else {
                  return failedFuture(
                      new IllegalStateException(
                          "More than one row deleted ("
                              + rows
                              + ") when performing delete"
                              + " on "
                              + entry.description()));
                }
              });
    } catch (Exception e) {
      return failedFuture(e);
    }
  }

  @Override
  public final CompletableFuture<Void> update(TX tx, TransactionOutboxEntry entry) {
    try {
      return sqlApi
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
          .exceptionally(
              e -> {
                throw sneakyRethrow(sqlApi.mapUpdateException(entry, e));
              })
          .thenCompose(
              rows -> {
                if (rows == 1) {
                  log.debug("Updated {}", entry.description());
                  entry.setVersion(entry.getVersion() + 1);
                  return completedFuture(null);
                } else if (rows == 0) {
                  return failedFuture(new OptimisticLockException());
                } else {
                  return failedFuture(
                      new IllegalStateException(
                          "More than one row updated ("
                              + rows
                              + ") when performing update"
                              + " on "
                              + entry.description()));
                }
              });
    } catch (Exception e) {
      return failedFuture(e);
    }
  }

  @Override
  public final CompletableFuture<Boolean> lock(TX tx, TransactionOutboxEntry entry) {
    try {
      return sqlApi
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
          .handle(
              (list, e) -> {
                if (e != null) {
                  Throwable t = sqlApi.mapLockException(entry, e);
                  if (t instanceof LockException) {
                    return false;
                  }
                  throw sneakyRethrow(t);
                }
                return !list.isEmpty();
              });
    } catch (Exception e) {
      return failedFuture(e);
    }
  }

  @Override
  public final CompletableFuture<Boolean> whitelist(TX tx, String entryId) {
    try {
      return sqlApi
          .statement(
              tx,
              dialect,
              whitelistSql,
              writeLockTimeoutSeconds,
              false,
              binder -> binder.bind(0, entryId).execute())
          .thenApply(rows -> rows != 0);
    } catch (Exception e) {
      return failedFuture(e);
    }
  }

  @Override
  public final CompletableFuture<List<TransactionOutboxEntry>> selectBatch(
      TX tx, int batchSize, Instant now) {
    try {
      return sqlApi.statement(
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
    try {
      return sqlApi.statement(
          tx,
          dialect,
          deleteExpiredSql,
          writeLockTimeoutSeconds,
          false,
          binder -> binder.bind(0, now).bind(1, batchSize).execute());
    } catch (Exception e) {
      return failedFuture(e);
    }
  }

  @Override
  public final CompletableFuture<Integer> clear(TX tx) {
    return sqlApi.statement(tx, dialect, clearSql, 0, false, SqlStatement::execute);
  }

  private CompletableFuture<Integer> createOutboxTableIfNotExists(TX tx) {
    return executeUpdate(
        tx,
        "CREATE TABLE IF NOT EXISTS TXNO_VERSION AS SELECT CAST(0 AS "
            + dialect.getIntegerCastType()
            + ") AS version");
  }

  private CompletableFuture<List<Integer>> fetchCurrentVersion(TX tx) {
    return sqlApi.statement(
        tx,
        dialect,
        "SELECT version FROM TXNO_VERSION FOR UPDATE",
        0,
        false,
        binder -> binder.executeQuery(1, row -> row.get(0, Integer.class)));
  }

  private CompletableFuture<Integer> updateVersion(TX tx, SqlMigration mig) {
    return executeUpdate(tx, "UPDATE TXNO_VERSION SET version = " + mig.getVersion());
  }

  private CompletableFuture<Integer> executeUpdate(TX tx, String sql) {
    return sqlApi.statement(
        tx, dialect, sql, writeLockTimeoutSeconds, false, SqlStatement::execute);
  }

  private String mapToNative(String sql) {
    return sqlApi.requiresNativeStatementMapping() ? dialect.mapStatementToNative(sql) : sql;
  }

  @SneakyThrows
  private RuntimeException sneakyRethrow(Throwable t) {
    throw t;
  }

  @Override
  public void onRegisterInitializationEvents(InitializationEventBus eventBus) {
    if (serializer instanceof InitializationEventSubscriber) {
      ((InitializationEventSubscriber) serializer).onRegisterInitializationEvents(eventBus);
    }
    if (sqlApi instanceof InitializationEventSubscriber) {
      ((InitializationEventSubscriber) sqlApi).onRegisterInitializationEvents(eventBus);
    }
  }

  @Beta
  public abstract static class SqlPersistorBuilder<
      CN, TX extends BaseTransaction<CN>, T extends SqlPersistorBuilder<CN, TX, T>> {
    private final SqlApi<CN, TX> sqlApi;
    private Integer writeLockTimeoutSeconds = 2;
    private Dialect dialect;
    private String tableName;
    private Boolean migrate;
    private InvocationSerializer serializer;

    protected SqlPersistorBuilder(SqlApi<CN, TX> sqlApi) {
      this.sqlApi = sqlApi;
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
          writeLockTimeoutSeconds, dialect, tableName, migrate, serializer, sqlApi);
    }
  }
}
