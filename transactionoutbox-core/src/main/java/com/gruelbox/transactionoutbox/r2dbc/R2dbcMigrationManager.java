package com.gruelbox.transactionoutbox.r2dbc;

import static com.ea.async.Async.await;
import static com.gruelbox.transactionoutbox.Utils.toRunningFuture;
import static java.util.concurrent.CompletableFuture.completedFuture;

import com.gruelbox.transactionoutbox.TransactionManager;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Result;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Non-blocking migration manager. Being non-blocking for upgrades isn't particularly important, but
 * we need to work when we only have an R2DBC connection available.
 */
@Slf4j
class R2dbcMigrationManager {

  /** Migrations are currently the same for all dialects so no disambiguation needed. */
  private static final List<Migration> MIGRATIONS =
      List.of(
          new Migration(
              1,
              "Create outbox table",
              "CREATE TABLE TXNO_OUTBOX (\n"
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
              "ALTER TABLE TXNO_OUTBOX ADD COLUMN uniqueRequestId VARCHAR(100) NULL UNIQUE"),
          new Migration(
              3, "Add processed flag", "ALTER TABLE TXNO_OUTBOX ADD COLUMN processed BOOLEAN"),
          new Migration(
              4,
              "Add flush index",
              "CREATE INDEX IX_TXNO_OUTBOX_1 ON TXNO_OUTBOX (processed, blacklisted, nextAttemptTime)"));

  static CompletableFuture<Void> migrate(
      TransactionManager<Connection, ?, ? extends R2dbcTransaction<?>> transactionManager) {
    return transactionManager.transactionally(
        tx -> {
          int currentVersion = await(currentVersion(tx.connection()));
          log.info("Current version is {}", currentVersion);
          for (Migration migration : MIGRATIONS) {
            if (migration.version <= currentVersion) {
              continue;
            }
            await(runMigration(tx.connection(), migration));
          }
          return completedFuture(null);
        });
  }

  private static CompletableFuture<Void> runMigration(Connection connection, Migration migration) {
    log.info("Running migration: {}", migration.name);
    await(runUpdate(connection, migration.sql));
    int versionUpdateRowsAffected =
        await(runUpdate(connection, "UPDATE TXNO_VERSION SET version = " + migration.version));
    if (versionUpdateRowsAffected != 1) {
      await(runUpdate(connection, "INSERT INTO TXNO_VERSION VALUES (" + migration.version + ")"));
    }
    return completedFuture(null);
  }

  private static CompletableFuture<Integer> runUpdate(Connection connection, String sql) {
    return runSql(connection, sql)
        .thenCompose((Result result) -> toRunningFuture(result.getRowsUpdated()));
  }

  private static CompletableFuture<? extends Result> runSql(Connection connection, String sql) {
    return toRunningFuture(connection.createStatement(sql).execute())
        .thenApply(
            result -> {
              log.debug("Ran SQL: {}", sql);
              return result;
            });
  }

  private static CompletableFuture<Integer> currentVersion(Connection connection) {
    log.info("Checking current version");
    await(createVersionTableIfNotExists(connection));
    Result result = await(runSql(connection, "SELECT version FROM TXNO_VERSION FOR UPDATE"));
    Integer version = await(toRunningFuture(result.map((row, meta) -> row.get(0, Integer.class))));
    return completedFuture(version == null ? 0 : version);
  }

  private static CompletableFuture<?> createVersionTableIfNotExists(Connection connection) {
    return runUpdate(connection, "CREATE TABLE IF NOT EXISTS TXNO_VERSION (version INT)");
  }

  @AllArgsConstructor
  private static final class Migration {
    private final int version;
    private final String name;
    private final String sql;
  }
}
