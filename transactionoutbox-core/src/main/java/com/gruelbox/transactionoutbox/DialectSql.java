package com.gruelbox.transactionoutbox;

/** An interface to support minor differences in SQL syntax. */
public interface DialectSql {
  Dialect getDialect();

  String lock(String tableName);

  String unblock(String tableName);

  String selectBatch(String tableName, String allFields, int batchSize);

  String deleteExpired(String tableName, int batchSize);

  boolean isSupportsSkipLock();
}
