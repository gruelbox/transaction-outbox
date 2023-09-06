package com.gruelbox.transactionoutbox;

/** Dialect SQL implementation for MySQL 8. */
public class DialectSqlMySQL8Impl extends DialectSqlMySQL5Impl {
  @Override
  public Dialect getDialect() {
    return Dialect.MY_SQL_8;
  }

  @Override
  public String lock(String tableName) {
    return "SELECT id, invocation FROM "
        + tableName
        + " WHERE id = ? AND version = ? FOR UPDATE SKIP LOCKED";
  }

  @Override
  public String selectBatch(String tableName, String allFields, int batchSize) {
    return super.selectBatch(tableName, allFields, batchSize) + " FOR UPDATE SKIP LOCKED";
  }

  @Override
  public boolean isSupportsSkipLock() {
    return true;
  }
}
