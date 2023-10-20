package com.gruelbox.transactionoutbox;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * The SQL dialects supported by {@link DefaultPersistor}. Currently this is only used to determine
 * whether {@code SKIP LOCKED} is available, so using the wrong dialect may work for unsupported
 * database platforms. However, in future this is likely to extend to other SQL features and
 * possibly be expanded to an interface to allow easier extension.
 */
@AllArgsConstructor
@Getter
@Beta
public enum Dialect {
  MY_SQL_5(
      false,
      false,
      false,
      Constants.DEFAULT_DELETE_EXPIRED_STMT,
      Constants.DEFAULT_LIMIT_CRITERIA,
      "now(6)"), //
  MY_SQL_8(
      true,
      true,
      true,
      Constants.DEFAULT_DELETE_EXPIRED_STMT,
      Constants.DEFAULT_LIMIT_CRITERIA,
      "now(6)"), //
  POSTGRESQL_9(
      true,
      true,
      true,
      "DELETE FROM {{table}} WHERE id IN (SELECT id FROM {{table}} WHERE nextAttemptTime < ? AND processed = true AND blocked = false LIMIT ?)",
      Constants.DEFAULT_LIMIT_CRITERIA,
      "statement_timestamp()"), //
  H2(
      false,
      true,
      true,
      Constants.DEFAULT_DELETE_EXPIRED_STMT,
      Constants.DEFAULT_LIMIT_CRITERIA,
      "current_timestamp(6)"), //
  ORACLE(
      true,
      true,
      false,
      "DELETE FROM {{table}} WHERE nextAttemptTime < ? AND processed = 1 AND blocked = 0 AND ROWNUM <= ?",
      Constants.ORACLE_LIMIT_CRITERIA,
      "current_timestamp(6)");

  /**
   * @return True if hot row support ({@code SKIP LOCKED}) is available, increasing performance when
   *     there are multiple instances of the application potentially competing to process the same
   *     task.
   */
  @SuppressWarnings("JavaDoc")
  private final boolean supportsSkipLock;

  /**
   * @return True if dialect supports use of SQL window functions.
   */
  @SuppressWarnings("JavaDoc")
  private final boolean supportsWindowFunctions;

  /**
   * @return True if dialect supports use of ({@code FOR UPDATE}) in queries using SQL window
   *     functions.
   */
  @SuppressWarnings("JavaDoc")
  private final boolean supportsWindowFunctionsForUpdate;

  /**
   * @return Format string for the SQL required to delete expired retained records.
   */
  @SuppressWarnings("JavaDoc")
  private final String deleteExpired;

  private final String limitCriteria;

  /**
   * @return SQL expression that returns the current timestamp.
   */
  @SuppressWarnings("JavaDoc")
  private final String currentTimestamp;

  private static class Constants {
    static final String DEFAULT_DELETE_EXPIRED_STMT =
        "DELETE FROM {{table}} WHERE nextAttemptTime < ? AND processed = true AND blocked = false LIMIT ?";

    static final String DEFAULT_LIMIT_CRITERIA = " LIMIT ?";

    static final String ORACLE_LIMIT_CRITERIA = " AND ROWNUM <= ?";
  }

  public String booleanValue(boolean criteriaValue) {
    String valueToReturn;
    if (this == ORACLE) valueToReturn = criteriaValue ? "1" : "0";
    else valueToReturn = criteriaValue ? Boolean.TRUE.toString() : Boolean.FALSE.toString();

    return valueToReturn;
  }
}
