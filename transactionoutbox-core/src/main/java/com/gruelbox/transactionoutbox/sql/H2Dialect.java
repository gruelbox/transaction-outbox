package com.gruelbox.transactionoutbox.sql;

import java.util.stream.Stream;

final class H2Dialect extends Dialect {

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
            "ALTER TABLE " + tableName + " ALTER COLUMN uniqueRequestId VARCHAR(250)"),
        new SqlMigration(
            6,
            "Rename column blacklisted to blocked",
            "ALTER TABLE " + tableName + " RENAME COLUMN blacklisted TO blocked"),
        new SqlMigration(
            7,
            "Add lastAttemptTime column to outbox",
            "ALTER TABLE "
                + tableName
                + " ADD COLUMN lastAttemptTime TIMESTAMP(6) NULL AFTER invocation"),
        new SqlMigration(
            8,
            "Make nextAttemptTime not null",
            "ALTER TABLE " + tableName + " ALTER COLUMN nextAttemptTime TIMESTAMP(6) NOT NULL"),
        new SqlMigration(
            9,
            "Fix data types on blocked and processed columns",
            String.format(
                "UPDATE %s SET blocked = false WHERE blocked IS NULL;\n"
                    + "UPDATE %s SET processed = false WHERE processed IS NULL;\n"
                    + "ALTER TABLE %s ALTER COLUMN processed BOOLEAN NOT NULL;\n"
                    + "ALTER TABLE %s ALTER COLUMN blocked BOOLEAN NOT NULL;",
                tableName, tableName, tableName, tableName)));
  }

  @Override
  public boolean isSupportsSkipLock() {
    return false;
  }

  @Override
  public String getIntegerCastType() {
    return "INT";
  }

  @Override
  public String getQueryTimeoutSetup() {
    return "SET QUERY_TIMEOUT ?";
  }
}
