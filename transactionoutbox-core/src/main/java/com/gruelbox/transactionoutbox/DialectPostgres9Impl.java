package com.gruelbox.transactionoutbox;

import lombok.EqualsAndHashCode;

/** Dialect SQL implementation for Postgres 9+. */
@EqualsAndHashCode
public class DialectPostgres9Impl extends DialectMySQL8Impl {

  @Override
  public String deleteExpired(String tableName, int batchSize) {
    return "DELETE FROM "
        + tableName
        + " WHERE id IN (SELECT id FROM "
        + tableName
        + " WHERE "
        + "nextAttemptTime < ? AND processed = ? AND blocked = ? LIMIT "
        + batchSize
        + ")";
  }
}
