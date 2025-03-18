package com.gruelbox.transactionoutbox;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class OracleDialect extends BaseDialect {
  public OracleDialect() {
    super();
    changeMigration(
        1,
        "CREATE TABLE TXNO_OUTBOX (\n"
            + "    id VARCHAR2(36) PRIMARY KEY,\n"
            + "    invocation CLOB,\n"
            + "    nextAttemptTime TIMESTAMP(6),\n"
            + "    attempts NUMBER,\n"
            + "    blacklisted NUMBER(1),\n"
            + "    version NUMBER\n"
            + ")");
    changeMigration(2, "ALTER TABLE TXNO_OUTBOX ADD uniqueRequestId VARCHAR(100) NULL UNIQUE");
    changeMigration(3, "ALTER TABLE TXNO_OUTBOX ADD processed NUMBER(1)");
    changeMigration(5, "ALTER TABLE TXNO_OUTBOX MODIFY uniqueRequestId VARCHAR2(250)");
    changeMigration(6, "ALTER TABLE TXNO_OUTBOX RENAME COLUMN blacklisted TO blocked");
    changeMigration(7, "ALTER TABLE TXNO_OUTBOX ADD lastAttemptTime TIMESTAMP(6)");
    disableMigration(8);
    changeMigration(9, "ALTER TABLE TXNO_OUTBOX ADD topic VARCHAR(250) DEFAULT '*' NOT NULL");
    changeMigration(10, "ALTER TABLE TXNO_OUTBOX ADD seq NUMBER");
    changeMigration(
        11,
        "CREATE TABLE TXNO_SEQUENCE (topic VARCHAR(250) NOT NULL, seq NUMBER NOT NULL, CONSTRAINT PK_TXNO_SEQUENCE PRIMARY KEY (topic, seq))");
  }

  @Override
  public String getName() {
    return "ORACLE";
  }

  @Override
  public String getFetchNextInAllTopics() {
    return "WITH cte1 AS (SELECT {{allFields}}, (ROW_NUMBER() OVER(PARTITION BY topic ORDER BY seq)) as rn"
        + " FROM {{table}} WHERE processed = 0 AND topic <> '*')"
        + " SELECT * FROM cte1 WHERE rn = 1 AND nextAttemptTime < ? AND ROWNUM <= {{batchSize}}";
  }

  @Override
  public String getLock() {
    return "SELECT id, invocation FROM {{table}} WHERE id = ? AND version = ? FOR "
        + "UPDATE SKIP LOCKED";
  }

  @Override
  public String getCheckSql() {
    return "SELECT 1 FROM DUAL";
  }

  @Override
  public String getDeleteExpired() {
    return "DELETE FROM {{table}} WHERE nextAttemptTime < ? AND processed = 1 AND blocked = 0 "
        + "AND ROWNUM <= {{batchSize}}";
  }

  @Override
  public String getSelectBatch() {
    return "SELECT {{allFields}} FROM {{table}} WHERE nextAttemptTime < ? "
        + "AND blocked = 0 AND processed = 0 AND topic = '*' AND ROWNUM <= {{batchSize}} FOR UPDATE "
        + "SKIP LOCKED";
  }

  @Override
  public void createVersionTableIfNotExists(Connection connection) throws SQLException {
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
  }

  @Override
  public String booleanValue(boolean criteriaValue) {
    return criteriaValue ? "1" : "0";
  }
}
