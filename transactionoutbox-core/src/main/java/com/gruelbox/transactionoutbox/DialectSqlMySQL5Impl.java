package com.gruelbox.transactionoutbox;

/** Dialect SQL implementation for MySQL 5. */
public class DialectSqlMySQL5Impl implements DialectSql {
  @Override
  public Dialect getDialect() {
    return Dialect.MY_SQL_5;
  }

  @Override
  public String lock(String tableName) {
    return "SELECT id, invocation FROM " + tableName + " WHERE id = ? AND version = ? FOR UPDATE";
  }

  @Override
  public String unblock(String tableName) {
    return "UPDATE "
        + tableName
        + " SET attempts = ?, blocked = ? "
        + "WHERE blocked = ? AND processed = ? AND id = ?";
  }

  @Override
  public String selectBatch(String tableName, String allFields, int batchSize) {
    return "SELECT "
        + allFields
        + " FROM "
        + tableName
        + " WHERE nextAttemptTime < ? AND blocked = ? AND processed = ? "
        + "LIMIT "
        + batchSize;
  }

  @Override
  public String deleteExpired(String tableName, int batchSize) {
    return "DELETE FROM "
        + tableName
        + " WHERE nextAttemptTime < ? AND processed = ? AND "
        + "blocked = ? LIMIT "
        + batchSize;
  }

  @Override
  public boolean isSupportsSkipLock() {
    return false;
  }
}
