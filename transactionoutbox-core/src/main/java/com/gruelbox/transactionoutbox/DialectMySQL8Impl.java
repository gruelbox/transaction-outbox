package com.gruelbox.transactionoutbox;

import lombok.EqualsAndHashCode;

/** Dialect SQL implementation for MySQL 8. */
@EqualsAndHashCode
public class DialectMySQL8Impl extends DialectMySQL5Impl {
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
