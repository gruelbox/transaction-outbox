package com.gruelbox.transactionoutbox;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class MsSQLServerDialect extends BaseDialect {
  public MsSQLServerDialect() {
    super();
    changeMigration(
        1,
        "CREATE TABLE TXNO_OUTBOX (\n"
            + "    id VARCHAR(36) PRIMARY KEY,\n"
            + "    invocation NVARCHAR(MAX),\n"
            + "    nextAttemptTime DATETIME2(6),\n"
            + "    attempts INT,\n"
            + "    blocked BIT,\n"
            + "    version INT,\n"
            + "    uniqueRequestId VARCHAR(250),\n"
            + "    processed BIT,\n"
            + "    lastAttemptTime DATETIME2(6),\n"
            + "    topic VARCHAR(250) DEFAULT '*' NOT NULL,\n"
            + "    seq INT\n"
            + ")");
    disableMigration(2);
    disableMigration(3);
    changeMigration(
        4, "CREATE INDEX IX_TXNO_OUTBOX_1 ON TXNO_OUTBOX (processed, blocked, nextAttemptTime)");
    disableMigration(5);
    disableMigration(6);
    disableMigration(7);
    changeMigration(
        8,
        "CREATE UNIQUE INDEX UX_TXNO_OUTBOX_uniqueRequestId ON TXNO_OUTBOX (uniqueRequestId) WHERE uniqueRequestId IS NOT NULL");
    disableMigration(9);
    disableMigration(10);
    changeMigration(
        11,
        "CREATE TABLE TXNO_SEQUENCE (topic VARCHAR(250) NOT NULL, seq INT NOT NULL, CONSTRAINT "
            + "PK_TXNO_SEQUENCE PRIMARY KEY (topic, seq))");
  }

  @Override
  public String getName() {
    return "MS_SQL_SERVER";
  }

  @Override
  public String getLock() {
    return "SELECT id, invocation FROM {{table}} WITH (UPDLOCK, ROWLOCK, READPAST) WHERE id = ? AND version = ?";
  }

  @Override
  public String getSelectBatch() {
    return "SELECT TOP ({{batchSize}}) {{allFields}} FROM {{table}} "
        + "WITH (UPDLOCK, ROWLOCK, READPAST) WHERE nextAttemptTime < ? AND topic = '*' "
        + "AND blocked = 0 AND processed = 0";
  }

  @Override
  public String getDelete() {
    return "DELETE FROM {{table}} WITH (ROWLOCK, READPAST) WHERE id = ? and version = ?";
  }

  @Override
  public String getDeleteExpired() {
    return "DELETE  TOP ({{batchSize}}) FROM {{table}} "
        + "WHERE nextAttemptTime < ? AND processed = 1 AND blocked = 0";
  }

  @Override
  public String getFetchCurrentVersion() {
    return "SELECT version FROM TXNO_VERSION WITH (UPDLOCK, ROWLOCK, READPAST)";
  }

  @Override
  public String getFetchNextSequence() {
    return "SELECT seq FROM TXNO_SEQUENCE WITH (UPDLOCK, ROWLOCK, READPAST) WHERE topic = ?";
  }

  @Override
  public String booleanValue(boolean criteriaValue) {
    return criteriaValue ? "1" : "0";
  }

  @Override
  public void createVersionTableIfNotExists(Connection connection) throws SQLException {
    try (Statement s = connection.createStatement()) {
      s.execute(
          "IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'TXNO_VERSION')\n"
              + "BEGIN\n"
              + "    CREATE TABLE TXNO_VERSION (\n"
              + "        version INT\n"
              + "    );"
              + "END");
    }
    ;
  }

  @Override
  public String getFetchNextInAllTopics() {
    return "SELECT TOP {{batchSize}} {{allFields}} FROM {{table}} a"
        + " WHERE processed = 0 AND topic <> '*' AND nextAttemptTime < ?"
        + " AND seq = ("
        + "SELECT MIN(seq) FROM {{table}} b WHERE b.topic=a.topic AND b.processed = 0"
        + ")";
  }
}
