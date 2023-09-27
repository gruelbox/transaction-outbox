package com.gruelbox.transactionoutbox;

import java.sql.SQLException;
import java.sql.Statement;
import lombok.EqualsAndHashCode;

/** Dialect SQL implementation for Postgres 9+. */
@EqualsAndHashCode
final class DialectPostgres9Impl implements Dialect {
  private final Dialect skipLockingBase = new DialectBaseSkipLockingImpl();

  @Override
  public String lock(String tableName) {
    return skipLockingBase.lock(tableName);
  }

  @Override
  public String unblock(String tableName) {
    return skipLockingBase.unblock(tableName);
  }

  @Override
  public String selectBatch(String tableName, String allFields, int batchSize) {
    return skipLockingBase.selectBatch(tableName, allFields, batchSize);
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

  @Override
  public boolean isSupportsSkipLock() {
    return skipLockingBase.isSupportsSkipLock();
  }

  @Override
  public void createVersionTableIfNotExists(Statement s) throws SQLException {
    skipLockingBase.createVersionTableIfNotExists(s);
  }
}
