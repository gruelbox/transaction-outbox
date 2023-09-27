package com.gruelbox.transactionoutbox;

import java.sql.SQLException;
import java.sql.Statement;
import lombok.EqualsAndHashCode;

/** Dialect SQL implementation for H2. */
@EqualsAndHashCode
final class DialectH2Impl implements Dialect {
  private final Dialect lockingBase = new DialectBaseLockingImpl();

  @Override
  public String lock(String tableName) {
    return lockingBase.lock(tableName);
  }

  @Override
  public String unblock(String tableName) {
    return lockingBase.unblock(tableName);
  }

  @Override
  public String selectBatch(String tableName, String allFields, int batchSize) {
    return lockingBase.selectBatch(tableName, allFields, batchSize);
  }

  @Override
  public String deleteExpired(String tableName, int batchSize) {
    return lockingBase.deleteExpired(tableName, batchSize);
  }

  @Override
  public boolean isSupportsSkipLock() {
    return lockingBase.isSupportsSkipLock();
  }

  @Override
  public void createVersionTableIfNotExists(Statement s) throws SQLException {
    lockingBase.createVersionTableIfNotExists(s);
  }
}
