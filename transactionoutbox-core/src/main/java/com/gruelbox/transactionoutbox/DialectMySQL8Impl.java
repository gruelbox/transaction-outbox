package com.gruelbox.transactionoutbox;

import java.sql.SQLException;
import java.sql.Statement;
import lombok.EqualsAndHashCode;

/** Dialect SQL implementation for MySQL 8. */
@EqualsAndHashCode
final class DialectMySQL8Impl implements Dialect {
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
    return skipLockingBase.deleteExpired(tableName, batchSize);
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
