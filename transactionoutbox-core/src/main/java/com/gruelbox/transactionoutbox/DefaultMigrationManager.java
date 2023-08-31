package com.gruelbox.transactionoutbox;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * Simple database migration manager. Inspired by Flyway, Liquibase, Morf etc, just trimmed down for
 * minimum dependencies.
 */
@Slf4j
class DefaultMigrationManager {

  /** Migrations can be dialect specific * */
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
                  + ")",
              Map.of(
                  Dialect.ORACLE,
                  "CREATE TABLE TXNO_OUTBOX (\n"
                      + "    id VARCHAR2(36) PRIMARY KEY,\n"
                      + "    invocation CLOB,\n"
                      + "    nextAttemptTime TIMESTAMP(6),\n"
                      + "    attempts NUMBER,\n"
                      + "    blacklisted NUMBER(1),\n"
                      + "    version NUMBER\n"
                      + ")",
                  Dialect.MSSQL,
                  "CREATE TABLE TXNO_OUTBOX (\n"
                      + "    id VARCHAR(36) PRIMARY KEY,\n"
                      + "    invocation NVARCHAR(MAX),\n"
                      + "    nextAttemptTime DATETIME2(6),\n"
                      + "    attempts INT,\n"
                      + "    blacklisted BIT,\n"
                      + "    version INT\n"
                      + ")")),
          new Migration(
              2,
              "Add unique request id",
              "ALTER TABLE TXNO_OUTBOX ADD COLUMN uniqueRequestId VARCHAR(100) NULL UNIQUE",
              Map.of(
                  Dialect.ORACLE,
                  "ALTER TABLE TXNO_OUTBOX ADD uniqueRequestId VARCHAR(100) NULL UNIQUE",
                  Dialect.MSSQL,
                  "ALTER TABLE TXNO_OUTBOX ADD uniqueRequestId VARCHAR(100)")),
          new Migration(
              3,
              "Add processed flag",
              "ALTER TABLE TXNO_OUTBOX ADD COLUMN processed BOOLEAN",
              Map.of(
                  Dialect.ORACLE,
                  "ALTER TABLE TXNO_OUTBOX ADD processed NUMBER(1)",
                  Dialect.MSSQL,
                  "ALTER TABLE TXNO_OUTBOX ADD processed BIT")),
          new Migration(
              4,
              "Add flush index",
              "CREATE INDEX IX_TXNO_OUTBOX_1 ON TXNO_OUTBOX (processed, blacklisted, "
                  + "nextAttemptTime)"),
          new Migration(
              5,
              "Increase size of uniqueRequestId",
              "ALTER TABLE TXNO_OUTBOX MODIFY COLUMN uniqueRequestId VARCHAR(250)",
              Map.of(
                  Dialect.POSTGRESQL_9,
                  "ALTER TABLE TXNO_OUTBOX ALTER COLUMN uniqueRequestId TYPE VARCHAR(250)",
                  Dialect.H2,
                  "ALTER TABLE TXNO_OUTBOX ALTER COLUMN uniqueRequestId VARCHAR(250)",
                  Dialect.ORACLE,
                  "ALTER TABLE TXNO_OUTBOX MODIFY uniqueRequestId VARCHAR2(250)",
                  Dialect.MSSQL,
                  "ALTER TABLE TXNO_OUTBOX ALTER COLUMN uniqueRequestId VARCHAR(250)")),
          new Migration(
              6,
              "Rename column blacklisted to blocked",
              "ALTER TABLE TXNO_OUTBOX CHANGE COLUMN blacklisted blocked VARCHAR(250)",
              Map.of(
                  Dialect.POSTGRESQL_9,
                  "ALTER TABLE TXNO_OUTBOX RENAME COLUMN blacklisted TO blocked",
                  Dialect.ORACLE,
                  "ALTER TABLE TXNO_OUTBOX RENAME COLUMN blacklisted TO blocked",
                  Dialect.H2,
                  "ALTER TABLE TXNO_OUTBOX RENAME COLUMN blacklisted TO blocked",
                  Dialect.MSSQL,
                  "EXEC sp_rename 'TXNO_OUTBOX.blacklisted', 'blocked', 'COLUMN'")),
          new Migration(
              7,
              "Add lastAttemptTime column to outbox",
              "ALTER TABLE TXNO_OUTBOX ADD COLUMN lastAttemptTime TIMESTAMP(6) NULL AFTER "
                  + "invocation",
              Map.of(
                  Dialect.POSTGRESQL_9,
                  "ALTER TABLE TXNO_OUTBOX ADD COLUMN lastAttemptTime TIMESTAMP(6)",
                  Dialect.ORACLE,
                  "ALTER TABLE TXNO_OUTBOX ADD lastAttemptTime TIMESTAMP(6)",
                  Dialect.MSSQL,
                  "ALTER TABLE TXNO_OUTBOX ADD lastAttemptTime DATETIME2(6)")),
          new Migration(
              8,
              "Update length of invocation column on outbox for MySQL dialects only.",
              "ALTER TABLE TXNO_OUTBOX MODIFY COLUMN invocation MEDIUMTEXT",
              Map.of(
                  Dialect.POSTGRESQL_9,
                  "",
                  Dialect.H2,
                  "",
                  Dialect.ORACLE,
                  "SELECT * FROM dual",
                  Dialect.MSSQL,
                  "")),
          new Migration(
              9,
              "Add unique constraint that allows multiple nulls for uniqueRequestId column on outbox for SQLServer dialects only.",
              "",
              Map.of(
                  Dialect.MSSQL,
                  "CREATE UNIQUE INDEX UX_TXNO_OUTBOX_uniqueRequestId ON TXNO_OUTBOX (uniqueRequestId) WHERE uniqueRequestId IS NOT NULL")));

  static void migrate(TransactionManager transactionManager, Dialect dialect) {
    transactionManager.inTransaction(
        transaction -> {
          try {
            int currentVersion = currentVersion(transaction.connection(), dialect);
            MIGRATIONS.stream()
                .filter(migration -> migration.version > currentVersion)
                .forEach(migration -> runSql(transaction.connection(), migration, dialect));
          } catch (Exception e) {
            throw new RuntimeException("Migrations failed", e);
          }
        });
  }

  @SneakyThrows
  private static void runSql(Connection connection, Migration migration, Dialect dialect) {
    log.info("Running migration: {}", migration.name);
    try (Statement s = connection.createStatement()) {
      String sql = migration.sqlFor(dialect);
      if (sql != null && !sql.isBlank()) {
        s.execute(sql);
      }
      if (s.executeUpdate("UPDATE TXNO_VERSION SET version = " + migration.version) != 1) {
        s.execute("INSERT INTO TXNO_VERSION VALUES (" + migration.version + ")");
      }
    }
  }

  private static int currentVersion(Connection connection, Dialect dialect) throws SQLException {
    createVersionTableIfNotExists(connection, dialect);
    String sql;
    switch (dialect) {
      case MSSQL:
        sql = "SELECT version FROM TXNO_VERSION WITH (updlock, holdlock, rowlock)";
        break;
      case H2:
      case ORACLE:
      case MY_SQL_5:
      case MY_SQL_8:
      case POSTGRESQL_9:
      default:
        sql = "SELECT version FROM TXNO_VERSION FOR UPDATE";
    }
    try (Statement s = connection.createStatement();
        ResultSet rs = s.executeQuery(sql)) {
      if (!rs.next()) {
        return 0;
      }
      return rs.getInt(1);
    }
  }

  private static void createVersionTableIfNotExists(Connection connection, Dialect dialect)
      throws SQLException {
    try (Statement s = connection.createStatement()) {
      switch (dialect) {
        case ORACLE:
          try {
            s.execute("CREATE TABLE TXNO_VERSION (version NUMBER)");
          } catch (SQLException e) {
            // oracle code for name already used by an existing object
            if (!e.getMessage().contains("955")) {
              throw e;
            }
          }
          break;
        case MSSQL:
          try {
            s.execute("CREATE TABLE TXNO_VERSION (version INT)");
          } catch (SQLException e) {
            if (e.getErrorCode() != 2714) {
              throw e;
            }
          }
          break;
        case MY_SQL_5:
        case H2:
        case MY_SQL_8:
        case POSTGRESQL_9:
        default:
          s.execute("CREATE TABLE IF NOT EXISTS TXNO_VERSION (version INT)");
          break;
      }
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
