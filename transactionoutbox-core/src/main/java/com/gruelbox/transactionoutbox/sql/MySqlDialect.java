package com.gruelbox.transactionoutbox.sql;

import java.util.stream.Stream;

class MySqlDialect extends Dialect {

  private final boolean supportsSkipLock;

  MySqlDialect(boolean supportsSkipLock) {
    this.supportsSkipLock = supportsSkipLock;
  }

  @Override
  public Stream<Migration> migrations(String tableName) {
    return Stream.of(
        new Migration(
            1,
            "Create outbox table",
            "CREATE TABLE "
                + tableName
                + " (\n"
                + "    id VARCHAR(36) PRIMARY KEY,\n"
                + "    invocation TEXT,\n"
                + "    nextAttemptTime TIMESTAMP(6),\n"
                + "    attempts INT,\n"
                + "    blacklisted BOOLEAN,\n"
                + "    version INT\n"
                + ")"),
        new Migration(
            2,
            "Add unique request id",
            "ALTER TABLE " + tableName + " ADD COLUMN uniqueRequestId VARCHAR(100) NULL UNIQUE"),
        new Migration(
            3, "Add processed flag", "ALTER TABLE " + tableName + " ADD COLUMN processed BOOLEAN"),
        new Migration(
            4,
            "Add flush index",
            "CREATE INDEX IX_"
                + tableName
                + "_1 "
                + "ON "
                + tableName
                + " (processed, blacklisted, nextAttemptTime)"),
        new Migration(
            5,
            "Use datetime datatype for the next process date",
            "ALTER TABLE " + tableName + " MODIFY COLUMN nextAttemptTime DATETIME(6) NOT NULL"));
  }

  @Override
  public boolean isSupportsSkipLock() {
    return supportsSkipLock;
  }

  @Override
  public String getIntegerCastType() {
    return "SIGNED";
  }

  @Override
  public String getQueryTimeoutSetup() {
    return "SET innodb_lock_wait_timeout = ?";
  }

  @Override
  public SqlResultRow mapResultFromNative(SqlResultRow row) {
    return new SqlResultRow() {
      @SuppressWarnings("unchecked")
      @Override
      public <T> T get(int index, Class<T> type) {
        if (Boolean.class.equals(type)) {
          return (T) Boolean.valueOf(row.get(index, Short.class) == 1);
        } else {
          return row.get(index, type);
        }
      }
    };
  }
}
