package com.gruelbox.transactionoutbox;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * Simple database migration manager. Inspired by Flyway, Liquibase, Morf etc, just trimmed down for
 * minimum dependencies.
 */
@Slf4j
class DefaultMigrationManager {

  /** Migrations can be dialect specific **/
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
              "CREATE INDEX IX_TXNO_OUTBOX_1 ON TXNO_OUTBOX (processed, blacklisted, nextAttemptTime)"),
          new Migration(
              5,
              "Increase size of uniqueRequestId",
              "ALTER TABLE TXNO_OUTBOX MODIFY COLUMN uniqueRequestId VARCHAR(250)",
              Map.of(
                  Dialect.POSTGRESQL_9,
                  "ALTER TABLE TXNO_OUTBOX ALTER COLUMN uniqueRequestId TYPE VARCHAR(250)",
                  Dialect.H2,
                  "ALTER TABLE TXNO_OUTBOX ALTER COLUMN uniqueRequestId VARCHAR(250)")),
          new Migration(
              6,
              "Rename column blacklisted to blocked",
              "ALTER TABLE TXNO_OUTBOX CHANGE COLUMN blacklisted blocked VARCHAR(250)",
              Map.of(
                  Dialect.POSTGRESQL_9,
                  "ALTER TABLE TXNO_OUTBOX RENAME COLUMN blacklisted TO blocked",
                  Dialect.H2,
                  "ALTER TABLE TXNO_OUTBOX RENAME COLUMN blacklisted TO blocked")));

  static void migrate(TransactionManager transactionManager, @NotNull Dialect dialect) {
    transactionManager.inTransaction(
        transaction -> {
          try {
            int currentVersion = currentVersion(transaction.connection());
            MIGRATIONS.stream()
                .filter(migration -> migration.version > currentVersion)
                .forEach(migration -> runSql(transaction.connection(), migration, dialect));
          } catch (Exception e) {
            throw new RuntimeException("Migrations failed", e);
          }
        });
  }

  @SneakyThrows
  private static void runSql(Connection connection, Migration migration, @NotNull Dialect dialect) {
    log.info("Running migration: {}", migration.name);
    try (Statement s = connection.createStatement()) {
      s.execute(migration.sqlFor(dialect));
      if (s.executeUpdate("UPDATE TXNO_VERSION SET version = " + migration.version) != 1) {
        s.execute("INSERT INTO TXNO_VERSION VALUES (" + migration.version + ")");
      }
    }
  }

  private static int currentVersion(Connection connection) throws SQLException {
    createVersionTableIfNotExists(connection);
    try (Statement s = connection.createStatement();
        ResultSet rs = s.executeQuery("SELECT version FROM TXNO_VERSION FOR UPDATE")) {
      if (!rs.next()) {
        return 0;
      }
      return rs.getInt(1);
    }
  }

  private static void createVersionTableIfNotExists(Connection connection) throws SQLException {
    try (Statement s = connection.createStatement()) {
      // language=MySQL
      s.execute("CREATE TABLE IF NOT EXISTS TXNO_VERSION (version INT)");
    }
  }

  @AllArgsConstructor
  private static final class Migration {
    private final int version;
    private final String name;
    private final String sql;
    private final Map<Dialect, String> dialectSpecific;

    Migration(int version, String name, String sql) {
      this(version, name, sql, Collections.emptyMap());
    }

    String sqlFor(Dialect dialect) {
      return dialectSpecific.getOrDefault(dialect, sql);
    }
  }
}
