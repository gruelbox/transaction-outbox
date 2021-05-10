package com.gruelbox.transactionoutbox.sql;

import java.util.stream.Stream;

final class MySqlDialect extends Dialect {

  private final boolean supportsSkipLock;

  MySqlDialect(boolean supportsSkipLock) {
    this.supportsSkipLock = supportsSkipLock;
  }

  @Override
  public Stream<SqlMigration> migrations(String tableName) {
    return Stream.of(
        new SqlMigration(
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
        new SqlMigration(
            2,
            "Add unique request id",
            "ALTER TABLE " + tableName + " ADD COLUMN uniqueRequestId VARCHAR(100) NULL UNIQUE"),
        new SqlMigration(
            3, "Add processed flag", "ALTER TABLE " + tableName + " ADD COLUMN processed BOOLEAN"),
        new SqlMigration(
            4,
            "Add flush index",
            "CREATE INDEX IX_"
                + tableName
                + "_1 "
                + "ON "
                + tableName
                + " (processed, blacklisted, nextAttemptTime)"),
        new SqlMigration(
            5,
            "Increase size of uniqueRequestId",
            "ALTER TABLE " + tableName + " MODIFY COLUMN uniqueRequestId VARCHAR(250)"),
        new SqlMigration(
            6,
            "Rename column blacklisted to blocked",
            "ALTER TABLE " + tableName + " CHANGE COLUMN blacklisted blocked VARCHAR(250)"),
        new SqlMigration(
            7,
            "Add lastAttemptTime column to outbox",
            "ALTER TABLE "
                + tableName
                + " ADD COLUMN lastAttemptTime TIMESTAMP(6) NULL AFTER invocation"),
        new SqlMigration(
            8,
            "Update length of invocation column on outbox for MySQL dialects only.",
            "ALTER TABLE TXNO_OUTBOX MODIFY COLUMN invocation MEDIUMTEXT"),
        new SqlMigration(
            9,
            "Use datetime datatype for the next process date",
            "ALTER TABLE " + tableName + " MODIFY COLUMN nextAttemptTime DATETIME(6) NOT NULL"),
        new SqlMigration(
            10,
            "Repair nulls on blocked column",
            String.format(
                "UPDATE %s SET blocked = CASE WHEN blocked = 'TRUE' or blocked = 'true' or blocked = '1' or blocked = 't' or blocked = 'y' or blocked = 'yes' or blocked = 'on' THEN '1' ELSE '0' END",
                tableName)),
        new SqlMigration(
            11,
            "Repair nulls on processed column",
            String.format("UPDATE %s SET processed = 0 WHERE processed IS NULL", tableName)),
        new SqlMigration(
            12,
            "Fix data types on blocked and processed columns",
            String.format(
                "ALTER TABLE %s\n"
                    + " MODIFY COLUMN processed BOOLEAN NOT NULL,\n"
                    + " MODIFY COLUMN blocked BOOLEAN NOT NULL",
                tableName)));
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
