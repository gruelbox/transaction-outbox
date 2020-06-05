package com.gruelbox.transactionoutbox.sql;

import java.util.stream.Stream;

class H2Dialect extends Dialect {

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
                + "    uniqueRequestId VARCHAR(100) NULL UNIQUE,\n"
                + "    invocation TEXT NOT NULL,\n"
                + "    nextAttemptTime TIMESTAMP(6) NOT NULL,\n"
                + "    attempts INT NOT NULL,\n"
                + "    processed BOOLEAN NOT NULL,\n"
                + "    blacklisted BOOLEAN NOT NULL,\n"
                + "    version INT NOT NULL\n"
                + ")"),
        new SqlMigration(
            2,
            "Add flush index",
            "CREATE INDEX IX_"
                + tableName
                + "_1 "
                + "ON "
                + tableName
                + " (processed, blacklisted, nextAttemptTime)"));
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
