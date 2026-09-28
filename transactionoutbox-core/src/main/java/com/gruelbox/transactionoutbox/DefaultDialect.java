package com.gruelbox.transactionoutbox;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
class DefaultDialect implements Dialect {

  static Builder builder(String name) {
    return new Builder(name);
  }

  @Getter private final String name;
  @Getter private final String deleteExpired;
  @Getter private final String delete;
  @Getter private final String selectBatch;
  @Getter private final String lock;
  @Getter private final String lockBatch;
  @Getter private final String checkSql;
  @Getter private final String fetchNextInAllTopics;
  @Getter private final String fetchNextInSelectedTopics;
  @Getter private final String fetchNextBatchInTopics;
  @Getter private final String fetchCurrentVersion;
  @Getter private final String fetchNextSequence;
  private final Collection<Migration> migrations;

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
  public String toString() {
    return name;
  }

  @Override
  public Stream<Migration> getMigrations() {
    return migrations.stream();
  }

  @Setter
  @Accessors(fluent = true)
  static final class Builder {
    private final String name;
    private String delete = "DELETE FROM {{table}} WHERE id = ? and version = ?";
    private String deleteExpired =
        "DELETE FROM {{table}} WHERE nextAttemptTime < ? AND processed = true AND blocked = false"
            + " LIMIT {{batchSize}}";
    private String selectBatch =
        "SELECT {{allFields}} FROM {{table}} WHERE nextAttemptTime < ? "
            + "AND blocked = false AND processed = false AND topic = '*' LIMIT {{batchSize}}";
    private String lock =
        "SELECT id, invocation FROM {{table}} WHERE id = ? AND version = ? FOR UPDATE";
    private String lockBatch =
        "SELECT id, version, invocation FROM {{table}} WHERE (id, version) IN ({{placeholders}}) FOR UPDATE";
    private String checkSql = "SELECT 1";
    private Map<Integer, Migration> migrations;
    private Function<Boolean, String> booleanValueFrom;
    private SQLAction createVersionTableBy;
    private String fetchNextInAllTopics =
        "SELECT {{allFields}} FROM {{table}} a"
            + " WHERE processed = false AND topic <> '*' AND nextAttemptTime < ?"
            + " AND seq = ("
            + "SELECT MIN(seq) FROM {{table}} b WHERE b.topic=a.topic AND b.processed = false"
            + ") LIMIT {{batchSize}}";
    private String fetchNextInSelectedTopics =
        "SELECT {{allFields}} FROM {{table}} a"
            + " WHERE processed = false AND topic IN ({{topicNames}}) AND nextAttemptTime < ?"
            + " AND seq = ("
            + "SELECT MIN(seq) FROM {{table}} b WHERE b.topic=a.topic AND b.processed = false"
            + ") LIMIT {{batchSize}}";
    private String fetchNextBatchInTopics =
        "WITH raw AS ("
            + " SELECT {{allFields}}, ROW_NUMBER() OVER (PARTITION BY topic ORDER BY seq) as rn"
            + " FROM {{table}}"
            + " WHERE processed = false AND topic <> '*'"
            + ")"
            + " SELECT * FROM raw WHERE rn <= {{batchSize}} AND nextAttemptTime < ?";
    private String fetchCurrentVersion = "SELECT version FROM TXNO_VERSION FOR UPDATE";
    private String fetchNextSequence = "SELECT seq FROM TXNO_SEQUENCE WHERE topic = ? FOR UPDATE";

    Builder(String name) {
      this.name = name;
      this.migrations = new TreeMap<>();
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
      migrations.put(13, new Migration(13, "Enforce UTF8 collation for outbox messages", null));
    }

    Builder setMigration(Migration migration) {
      this.migrations.put(migration.getVersion(), migration);
      return this;
    }

    Builder changeMigration(int version, String sql) {
      return setMigration(this.migrations.get(version).withSql(sql));
    }

    Builder disableMigration(@SuppressWarnings("SameParameterValue") int version) {
      return setMigration(this.migrations.get(version).withSql(null));
    }

    Dialect build() {
      return new DefaultDialect(
          name,
          deleteExpired,
          delete,
          selectBatch,
          lock,
          lockBatch,
          checkSql,
          fetchNextInAllTopics,
          fetchNextInSelectedTopics,
          fetchNextBatchInTopics,
          fetchCurrentVersion,
          fetchNextSequence,
          migrations.values()) {
        @Override
        public String booleanValue(boolean criteriaValue) {
          if (booleanValueFrom != null) {
            return booleanValueFrom.apply(criteriaValue);
          }
          return super.booleanValue(criteriaValue);
        }

        @Override
        public void createVersionTableIfNotExists(Connection connection) throws SQLException {
          if (createVersionTableBy != null) {
            createVersionTableBy.doAction(connection);
          } else {
            super.createVersionTableIfNotExists(connection);
          }
        }
      };
    }
  }
}
