package com.gruelbox.transactionoutbox;

import java.sql.SQLException;
import java.sql.Statement;

final class DialectBaseSkipLockingImpl implements Dialect {
  @Override
  public String lock(String tableName) {
    return "SELECT id, invocation FROM "
        + tableName
        + " WHERE id = ? AND version = ? FOR UPDATE SKIP LOCKED";
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
        + batchSize
        + " FOR UPDATE SKIP LOCKED";
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
    return true;
  }

  @Override
  public void createVersionTableIfNotExists(Statement s) throws SQLException {
    s.execute("CREATE TABLE IF NOT EXISTS TXNO_VERSION (version INT)");
  }
}
