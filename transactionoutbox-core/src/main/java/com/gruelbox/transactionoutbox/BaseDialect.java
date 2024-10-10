package com.gruelbox.transactionoutbox;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

public abstract class BaseDialect implements Dialect {
  private final Map<Integer, Migration> migrations = new TreeMap<>();

  BaseDialect() {
    migrations.put(
        1,
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
                + ")"));
    migrations.put(
        2,
        new Migration(
            2,
            "Add unique request id",
            "ALTER TABLE TXNO_OUTBOX ADD COLUMN uniqueRequestId VARCHAR(100) NULL UNIQUE"));
    migrations.put(
        3,
        new Migration(
            3, "Add processed flag", "ALTER TABLE TXNO_OUTBOX ADD COLUMN processed BOOLEAN"));
    migrations.put(
        4,
        new Migration(
            4,
            "Add flush index",
            "CREATE INDEX IX_TXNO_OUTBOX_1 ON TXNO_OUTBOX (processed, blacklisted, nextAttemptTime)"));
    migrations.put(
        5,
        new Migration(
            5,
            "Increase size of uniqueRequestId",
            "ALTER TABLE TXNO_OUTBOX MODIFY COLUMN uniqueRequestId VARCHAR(250)"));
    migrations.put(
        6,
        new Migration(
            6,
            "Rename column blacklisted to blocked",
            "ALTER TABLE TXNO_OUTBOX CHANGE COLUMN blacklisted blocked VARCHAR(250)"));
    migrations.put(
        7,
        new Migration(
            7,
            "Add lastAttemptTime column to outbox",
            "ALTER TABLE TXNO_OUTBOX ADD COLUMN lastAttemptTime TIMESTAMP(6) NULL AFTER invocation"));
    migrations.put(
        8,
        new Migration(
            8,
            "Update length of invocation column on outbox for MySQL dialects only.",
            "ALTER TABLE TXNO_OUTBOX MODIFY COLUMN invocation MEDIUMTEXT"));
    migrations.put(
        9,
        new Migration(
            9,
            "Add topic",
            "ALTER TABLE TXNO_OUTBOX ADD COLUMN topic VARCHAR(250) NOT NULL DEFAULT '*'"));
    migrations.put(
        10,
        new Migration(10, "Add sequence", "ALTER TABLE TXNO_OUTBOX ADD COLUMN seq BIGINT NULL"));
    migrations.put(
        11,
        new Migration(
            11,
            "Add sequence table",
            "CREATE TABLE TXNO_SEQUENCE (topic VARCHAR(250) NOT NULL, seq BIGINT NOT NULL, PRIMARY KEY (topic, seq))"));
    migrations.put(
        12,
        new Migration(
            12,
            "Add flush index to support ordering",
            "CREATE INDEX IX_TXNO_OUTBOX_2 ON TXNO_OUTBOX (topic, processed, seq)"));
  }

  @Override
  public String getName() {
    return "BaseDialect";
  }

  @Override
  public String getDelete() {
    return "DELETE FROM {{table}} WHERE id = ? and version = ?";
  }

  @Override
  public String getDeleteExpired() {
    return "DELETE FROM {{table}} WHERE nextAttemptTime < ? AND processed = true AND blocked = false"
        + " LIMIT {{batchSize}}";
  }

  @Override
  public String getSelectBatch() {
    return "SELECT {{allFields}} FROM {{table}} WHERE nextAttemptTime < ? "
        + "AND blocked = false AND processed = false AND topic = '*' LIMIT {{batchSize}}";
  }

  @Override
  public String getLock() {
    return "SELECT id, invocation FROM {{table}} WHERE id = ? AND version = ? FOR UPDATE";
  }

  @Override
  public String getCheckSql() {
    return "SELECT 1";
  }

  @Override
  public String getFetchNextInAllTopics() {
    return "SELECT {{allFields}} FROM {{table}} a"
        + " WHERE processed = false AND topic <> '*' AND nextAttemptTime < ?"
        + " AND seq = ("
        + "SELECT MIN(seq) FROM {{table}} b WHERE b.topic=a.topic AND b.processed = false"
        + ") LIMIT {{batchSize}}";
  }

  @Override
  public String getFetchCurrentVersion() {
    return "SELECT version FROM TXNO_VERSION FOR UPDATE";
  }

  @Override
  public String getFetchNextSequence() {
    return "SELECT seq FROM TXNO_SEQUENCE WHERE topic = ? FOR UPDATE";
  }

  @Override
  public String booleanValue(boolean criteriaValue) {
    return criteriaValue ? Boolean.TRUE.toString() : Boolean.FALSE.toString();
  }

  @Override
  public void createVersionTableIfNotExists(Connection connection) throws SQLException {
    try (Statement s = connection.createStatement()) {
      s.execute(
          "CREATE TABLE IF NOT EXISTS TXNO_VERSION (id INT DEFAULT 0, version INT, PRIMARY KEY (id))");
    }
  }

  @Override
  public Stream<Migration> getMigrations() {
    return migrations.values().stream();
  }

  void changeMigration(int version, String sql) {
    this.migrations.put(version, this.migrations.get(version).withSql(sql));
  }

  void disableMigration(int version) {
    this.migrations.put(version, this.migrations.get(version).withSql(null));
  }
}
