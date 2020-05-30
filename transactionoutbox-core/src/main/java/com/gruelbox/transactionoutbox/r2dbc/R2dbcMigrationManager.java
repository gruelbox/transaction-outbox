package com.gruelbox.transactionoutbox.r2dbc;

import com.gruelbox.transactionoutbox.TransactionManager;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Result;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

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

  static void migrate(
      TransactionManager<Connection, ?, ? extends R2dbcTransaction<?>> transactionManager) {
    Scheduler blockingScheduler = Schedulers.fromExecutor(ForkJoinPool.commonPool());
    transactionManager
        .transactionally(
            tx ->
                currentVersion(tx.connection())
                    .publishOn(blockingScheduler)
                    .doOnNext(currentVersion -> log.info("Current version is {}", currentVersion))
                    .doOnNext(
                        currentVersion -> {
                          MIGRATIONS.stream()
                              .filter(it -> it.version > currentVersion)
                              .forEach(
                                  migration -> runMigration(tx.connection(), migration).block());
                          log.info("Migrations complete");
                        })
                    .then()
                    .toFuture())
        .join();
  }

  private static Mono<Void> runMigration(Connection connection, Migration migration) {
    return Mono.fromRunnable(() -> log.info("Running migration: {}", migration.name))
        .defaultIfEmpty(1)
        .flatMap(__ -> runUpdate(connection, migration.sql))
        .flatMap(
            __ -> runUpdate(connection, "UPDATE TXNO_VERSION SET version = " + migration.version))
        .flatMap(
            rows -> {
              if (rows == 1) {
                return Mono.just(1);
              } else {
                return runUpdate(
                    connection, "INSERT INTO TXNO_VERSION VALUES (" + migration.version + ")");
              }
            })
        .then();
  }

  private static Mono<Integer> runUpdate(Connection connection, String sql) {
    return runSql(connection, sql)
        .flatMap(result -> Mono.from(result.getRowsUpdated()))
        .defaultIfEmpty(0)
        .doOnNext(rows -> log.debug("{} rows updated", rows));
  }

  private static Mono<? extends Result> runSql(Connection connection, String sql) {
    return Mono.fromRunnable(() -> log.info("Running SQL: {}", sql))
        .then(Mono.from(connection.createStatement(sql).execute()))
        .map(r -> (io.r2dbc.spi.Result) r)
        .defaultIfEmpty(Utils.EMPTY_RESULT)
        .doOnNext(__ -> log.debug("Ran SQL: {}", sql))
        .doOnError(e -> log.error("Error in SQL", e));
  }

  private static Mono<Integer> currentVersion(Connection connection) {
    return createVersionTableIfNotExists(connection)
        .flatMap(__ -> runSql(connection, "SELECT version FROM TXNO_VERSION FOR UPDATE"))
        .flatMap(result -> Mono.from(result.map((row, meta) -> row.get(0, Integer.class))))
        .defaultIfEmpty(0)
        .map(version -> version == null ? 0 : version);
  }

  private static Mono<?> createVersionTableIfNotExists(Connection connection) {
    return runUpdate(connection, "CREATE TABLE IF NOT EXISTS TXNO_VERSION (version INT)");
  }

  @AllArgsConstructor
  private static final class Migration {
    private final int version;
    private final String name;
    private final String sql;
  }
}
