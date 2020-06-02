package com.gruelbox.transactionoutbox.sql;

import java.util.stream.Stream;

class PostgreSqlDialect extends Dialect {

  private final boolean supportsSkipLock;

  public PostgreSqlDialect(boolean supportsSkipLock) {
    super(DialectFamily.POSTGRESQL);
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
            "Make nextAttemptTime not null",
            "ALTER TABLE " + tableName + " ALTER COLUMN nextAttemptTime SET NOT NULL"));
  }

  @Override
  public String getDeleteExpired() {
    return "DELETE FROM {{table}} "
        + "WHERE id IN ("
        + "  SELECT id FROM {{table}} "
        + "  WHERE nextAttemptTime < ? AND "
        + "        processed = true AND "
        + "        blacklisted = false "
        + "  LIMIT ?)";
  }

  @Override
  public boolean isSupportsSkipLock() {
    return supportsSkipLock;
  }

  @Override
  public String getIntegerCastType() {
    return "INTEGER";
  }

  @Override
  public String getQueryTimeoutSetup() {
    return "SET LOCAL lock_timeout = '{{timeout}}s'";
  }
}
