package com.gruelbox.transactionoutbox.sql;

import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.stream.Stream;

final class OracleDialect extends Dialect {

  @Override
  public Stream<SqlMigration> migrations(String tableName) {
    return Stream.of(
        new SqlMigration(
            1,
            "Create outbox table",
            "CREATE TABLE "
                + tableName
                + " (\n"
                + "    id VARCHAR2(36) PRIMARY KEY,\n"
                + "    invocation CLOB,\n"
                + "    nextAttemptTime TIMESTAMP(6),\n"
                + "    attempts NUMBER,\n"
                + "    blacklisted NUMBER(1),\n"
                + "    version NUMBER\n"
                + ")"),
        new SqlMigration(
            2,
            "Add unique request id",
            "ALTER TABLE " + tableName + " ADD uniqueRequestId VARCHAR(100) NULL UNIQUE"),
        new SqlMigration(
            3, "Add processed flag", "ALTER TABLE " + tableName + " ADD processed NUMBER(1)"),
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
            "ALTER TABLE " + tableName + " MODIFY uniqueRequestId VARCHAR2(250)"),
        new SqlMigration(
            6,
            "Rename column blacklisted to blocked",
            "ALTER TABLE " + tableName + " RENAME COLUMN blacklisted TO blocked"),
        new SqlMigration(
            7,
            "Add lastAttemptTime column to outbox",
            "ALTER TABLE " + tableName + " ADD lastAttemptTime TIMESTAMP(6)"),
        new SqlMigration(
            8,
            "Update length of invocation column on outbox for MySQL dialects only.",
            "SELECT * FROM dual"),
        new SqlMigration(
            9,
            "Make nextAttemptTime not null",
            "ALTER TABLE " + tableName + " MODIFY nextAttemptTime TIMESTAMP(6) NOT NULL"),
        new SqlMigration(
            10,
            "Fix data types on blocked and processed columns - part 1",
            String.format("UPDATE %s SET blocked = 0 WHERE blocked IS NULL", tableName)),
        new SqlMigration(
            11,
            "Fix data types on blocked and processed columns - part 2",
            String.format("UPDATE %s SET processed = 0 WHERE processed IS NULL", tableName)),
        new SqlMigration(
            12,
            "Fix data types on blocked and processed columns - part 3",
            String.format("ALTER TABLE %s MODIFY processed NUMBER(1) NOT NULL", tableName)),
        new SqlMigration(
            13,
            "Fix data types on blocked and processed columns - part 4",
            String.format("ALTER TABLE %s MODIFY blocked NUMBER(1) NOT NULL", tableName)));
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

  @Override
  public String getDeleteExpired() {
    return "DELETE FROM {{table}} WHERE nextAttemptTime < ? AND processed = 1 AND blocked = 0 AND ROWNUM <= ?";
  }

  @Override
  public String getLimitCriteria() {
    return " AND ROWNUM <= ?";
  }

  @Override
  public String getConnectionCheck() {
    return "SELECT 1 FROM DUAL";
  }

  @Override
  public String booleanValue(boolean criteriaValue) {
    return criteriaValue ? "1" : "0";
  }

  @Override
  public CompletableFuture<Long> createVersionTableIfNotExists(
      Function<String, CompletableFuture<Long>> statementInvoker) {
    return statementInvoker
        .apply("CREATE TABLE TXNO_VERSION AS SELECT CAST(0 AS NUMBER) as version FROM DUAL")
        .exceptionally(
            t -> {
              if (t instanceof CompletionException) {
                t = t.getCause();
              }
              if (t instanceof SQLException && t.getMessage().contains("955")) {
                return 0L;
              }
              if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
              }
              throw new CompletionException(t);
            });
  }
}
