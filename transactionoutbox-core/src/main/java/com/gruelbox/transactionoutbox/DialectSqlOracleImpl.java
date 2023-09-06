package com.gruelbox.transactionoutbox;

/** Dialect SQL implementation for Oracle. */
public class DialectSqlOracleImpl extends DialectSqlMySQL8Impl {
  @Override
  public Dialect getDialect() {
    return Dialect.ORACLE;
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
}
