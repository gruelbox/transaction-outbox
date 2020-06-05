package com.gruelbox.transactionoutbox.sql;

import java.util.stream.Stream;

class PostgreSqlDialect extends Dialect {

  private final boolean supportsSkipLock;

  PostgreSqlDialect(boolean supportsSkipLock) {
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
            "Make nextAttemptTime not null",
            "ALTER TABLE " + tableName + " ALTER COLUMN nextAttemptTime SET NOT NULL"));
  }

  @Override
  public String getDeleteExpired() {
    return "DELETE FROM {{table}}"
        + " WHERE id IN ("
        + "  SELECT id FROM {{table}}"
        + "  WHERE nextAttemptTime < ? AND"
        + "        processed = true AND"
        + "        blacklisted = false"
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

  @Override
  public String mapStatementToNative(String sql) {
    // Blunt, not general purpose, but only needs to work for the known use cases
    StringBuilder builder = new StringBuilder();
    int paramIndex = 1;
    for (int i = 0; i < sql.length(); i++) {
      char c = sql.charAt(i);
      if (c == '?') {
        builder.append('$').append(paramIndex++);
      } else {
        builder.append(c);
      }
    }
    return builder.toString();
  }
}
