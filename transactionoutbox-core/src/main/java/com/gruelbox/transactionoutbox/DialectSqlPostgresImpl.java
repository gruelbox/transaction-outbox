package com.gruelbox.transactionoutbox;

/** Dialect SQL implementation for Postgres 9+. */
public class DialectSqlPostgresImpl extends DialectSqlMySQL8Impl {
  @Override
  public Dialect getDialect() {
    return Dialect.POSTGRESQL_9;
  }

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
