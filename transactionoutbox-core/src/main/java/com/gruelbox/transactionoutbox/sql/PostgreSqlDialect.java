package com.gruelbox.transactionoutbox.sql;

import java.util.stream.Stream;

final class PostgreSqlDialect extends Dialect {

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
            "Increase size of uniqueRequestId",
            "ALTER TABLE " + tableName + " ALTER COLUMN uniqueRequestId TYPE VARCHAR(250)"),
        new SqlMigration(
            6,
            "Rename column blacklisted to blocked",
            "ALTER TABLE " + tableName + " RENAME COLUMN blacklisted TO blocked"),
        new SqlMigration(
            7,
            "Add lastAttemptTime column to outbox",
            "ALTER TABLE " + tableName + " ADD COLUMN lastAttemptTime TIMESTAMP(6) NULL"),
        new SqlMigration(
            8,
            "Make nextAttemptTime not null",
            "ALTER TABLE " + tableName + " ALTER COLUMN nextAttemptTime SET NOT NULL"),
        new SqlMigration(
            9,
            "Fix data types on blocked and processed columns",
            String.format(
                "UPDATE %s SET processed = FALSE WHERE processed IS NULL;\n"
                    + "ALTER TABLE %s\n"
                    + " ALTER COLUMN processed SET NOT NULL,\n"
                    + " ALTER COLUMN blocked TYPE BOOLEAN \n"
                    + "  USING CASE WHEN blocked = 'TRUE' or blocked = 'true' or blocked = '1' or blocked = 't' or blocked = 'y' or blocked = 'yes' or blocked = 'on' THEN TRUE ELSE FALSE END,\n"
                    + " ALTER COLUMN blocked SET NOT NULL;",
                tableName, tableName)));
  }

  @Override
  public String getDeleteExpired() {
    return "DELETE FROM {{table}}"
        + " WHERE id IN ("
        + "  SELECT id FROM {{table}}"
        + "  WHERE nextAttemptTime < ? AND"
        + "        processed = true AND"
        + "        blocked = false"
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
