package com.gruelbox.transactionoutbox;

import java.sql.SQLException;
import java.sql.Statement;
import lombok.EqualsAndHashCode;

/** Dialect SQL implementation for Oracle. */
@EqualsAndHashCode
final class DialectOracleImpl implements Dialect {
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
    return "SELECT "
        + allFields
        + " FROM "
        + tableName
        + " WHERE nextAttemptTime < ? AND blocked = ? AND processed = ? "
        + " AND ROWNUM <= "
        + batchSize
        + " FOR UPDATE SKIP LOCKED";
  }

  @Override
  public String deleteExpired(String tableName, int batchSize) {
    return "DELETE FROM "
        + tableName
        + " WHERE nextAttemptTime < ? AND processed = ? AND blocked = ?"
        + " AND ROWNUM <= "
        + batchSize;
  }

  @Override
  public boolean isSupportsSkipLock() {
    return skipLockingBase.isSupportsSkipLock();
  }

  @Override
  public void createVersionTableIfNotExists(Statement s) throws SQLException {
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
