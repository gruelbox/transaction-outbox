package com.gruelbox.transactionoutbox;

public class Postgresql9Dialect extends BaseDialect {
  public Postgresql9Dialect() {
    super();
    changeMigration(5, "ALTER TABLE TXNO_OUTBOX ALTER COLUMN uniqueRequestId TYPE VARCHAR(250)");
    changeMigration(6, "ALTER TABLE TXNO_OUTBOX RENAME COLUMN blacklisted TO blocked");
    changeMigration(7, "ALTER TABLE TXNO_OUTBOX ADD COLUMN lastAttemptTime TIMESTAMP(6)");
    disableMigration(8);
  }

  @Override
  public String getName() {
    return "POSTGRESQL_9";
  }

  @Override
  public String getDeleteExpired() {
    return "DELETE FROM {{table}} WHERE id IN "
        + "(SELECT id FROM {{table}} WHERE nextAttemptTime < ? AND processed = true AND blocked = false LIMIT {{batchSize}})";
  }

  @Override
  public String getSelectBatch() {
    return "SELECT {{allFields}} FROM {{table}} WHERE nextAttemptTime < ? "
        + "AND blocked = false AND processed = false AND topic = '*' LIMIT "
        + "{{batchSize}} FOR UPDATE SKIP LOCKED";
  }

  @Override
  public String getLock() {
    return "SELECT id, invocation FROM {{table}} WHERE id = ? AND version = ? FOR "
        + "UPDATE SKIP LOCKED";
  }

  @Override
  public String getFetchNextInAllTopics() {
    return "WITH raw AS(SELECT {{allFields}}, (ROW_NUMBER() OVER(PARTITION BY topic ORDER BY seq)) as rn"
        + " FROM {{table}} WHERE processed = false AND topic <> '*')"
        + " SELECT * FROM raw WHERE rn = 1 AND nextAttemptTime < ? LIMIT {{batchSize}}";
  }
}
