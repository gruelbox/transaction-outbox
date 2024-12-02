package com.gruelbox.transactionoutbox;

public class MySQL8Dialect extends BaseDialect {
  @Override
  public String getName() {
    return "MY_SQL_8";
  }

  @Override
  public String getFetchNextInAllTopics() {
    return "WITH raw AS(SELECT {{allFields}}, (ROW_NUMBER() OVER(PARTITION BY topic ORDER BY seq)) as rn"
        + " FROM {{table}} WHERE processed = false AND topic <> '*')"
        + " SELECT * FROM raw WHERE rn = 1 AND nextAttemptTime < ? LIMIT {{batchSize}}";
  }

  @Override
  public String getSelectBatch() {
    return "SELECT {{allFields}} FROM {{table}} WHERE nextAttemptTime < ? "
        + "AND blocked = false AND processed = false AND topic = '*' LIMIT {{batchSize}} FOR UPDATE "
        + "SKIP LOCKED";
  }

  @Override
  public String getLock() {
    return "SELECT id, invocation FROM {{table}} WHERE id = ? AND version = ? FOR "
        + "UPDATE SKIP LOCKED";
  }
}
