package com.gruelbox.transactionoutbox;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Stream;

/** The SQL dialects supported by {@link DefaultPersistor}. */
public interface Dialect {

  /**
   * @return True if hot row support ({@code SKIP LOCKED}) is available, increasing performance when
   *     there are multiple instances of the application potentially competing to process the same
   *     task.
   */
  boolean isSupportsSkipLock();

  /**
   * @return True if window functions are supported. This improves performance when using ordered
   * processing.
   */
  boolean isSupportsWindowFunctions();

  /**
   * @return Format string for the SQL required to delete expired retained records.
   */
  String getDeleteExpired();

  String getLimitCriteria();

  String getCheckSql();

  String booleanValue(boolean criteriaValue);

  void createVersionTableIfNotExists(Connection connection) throws SQLException;

  Stream<Migration> getMigrations();

  Dialect MY_SQL_5 = DefaultDialect.builder("MY_SQL_5").build();
  Dialect MY_SQL_8 = DefaultDialect.builder("MY_SQL_8").supportsSkipLock(true).supportsWindowFunctions(true).build();
  Dialect POSTGRESQL_9 =
      DefaultDialect.builder("POSTGRESQL_9")
          .supportsSkipLock(true)
          .supportsWindowFunctions(true)
          .deleteExpired(
              "DELETE FROM {{table}} WHERE id IN (SELECT id FROM {{table}} WHERE nextAttemptTime < ? AND processed = true AND blocked = false LIMIT ?)")
          .changeMigration(
              5, "ALTER TABLE TXNO_OUTBOX ALTER COLUMN uniqueRequestId TYPE VARCHAR(250)")
          .changeMigration(6, "ALTER TABLE TXNO_OUTBOX RENAME COLUMN blacklisted TO blocked")
          .changeMigration(7, "ALTER TABLE TXNO_OUTBOX ADD COLUMN lastAttemptTime TIMESTAMP(6)")
          .disableMigration(8)
          .build();

  Dialect H2 =
      DefaultDialect.builder("H2")
          .supportsWindowFunctions(true)
          .changeMigration(5, "ALTER TABLE TXNO_OUTBOX ALTER COLUMN uniqueRequestId VARCHAR(250)")
          .changeMigration(6, "ALTER TABLE TXNO_OUTBOX RENAME COLUMN blacklisted TO blocked")
          .disableMigration(8)
          .build();
  Dialect ORACLE =
      DefaultDialect.builder("ORACLE")
          .supportsWindowFunctions(true)
          .supportsSkipLock(true)
          .deleteExpired(
              "DELETE FROM {{table}} WHERE nextAttemptTime < ? AND processed = 1 AND blocked = 0 AND ROWNUM <= ?")
          .limitCriteria(" AND ROWNUM <= ?")
          .checkSql("SELECT 1 FROM DUAL")
          .changeMigration(
              1,
              "CREATE TABLE TXNO_OUTBOX (\n"
                  + "    id VARCHAR2(36) PRIMARY KEY,\n"
                  + "    invocation CLOB,\n"
                  + "    nextAttemptTime TIMESTAMP(6),\n"
                  + "    attempts NUMBER,\n"
                  + "    blacklisted NUMBER(1),\n"
                  + "    version NUMBER\n"
                  + ")")
          .changeMigration(
              2, "ALTER TABLE TXNO_OUTBOX ADD uniqueRequestId VARCHAR(100) NULL UNIQUE")
          .changeMigration(3, "ALTER TABLE TXNO_OUTBOX ADD processed NUMBER(1)")
          .changeMigration(5, "ALTER TABLE TXNO_OUTBOX MODIFY uniqueRequestId VARCHAR2(250)")
          .changeMigration(6, "ALTER TABLE TXNO_OUTBOX RENAME COLUMN blacklisted TO blocked")
          .changeMigration(7, "ALTER TABLE TXNO_OUTBOX ADD lastAttemptTime TIMESTAMP(6)")
          .disableMigration(8)
          .booleanValueFrom(v -> v ? "1" : "0")
          .createVersionTableBy(
              connection -> {
                try (Statement s = connection.createStatement()) {
                  try {
                    s.execute("CREATE TABLE TXNO_VERSION (version NUMBER)");
                  } catch (SQLException e) {
                    // oracle code for name already used by an existing object
                    if (!e.getMessage().contains("955")) {
                      throw e;
                    }
                  }
                }
              })
          .build();
}
