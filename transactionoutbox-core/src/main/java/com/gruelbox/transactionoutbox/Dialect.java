package com.gruelbox.transactionoutbox;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * The SQL dialects supported. Currently this is only used to determine whether {@code SKIP LOCKED}
 * is available, so using the wrong dialect may work for unsupported database platforms. However, in
 * future this is likely to extend to other SQL features and possibly be expanded to an interface to
 * allow easier extension.
 *
 * <p>Note that all properties of the dialect are very much under iteration; not much design has
 * gone into them; they've only need added as necessary to make things work.
 */
@AllArgsConstructor
@Getter
@Beta
public enum Dialect {
  MY_SQL_5(
      DialectFamily.MY_SQL,
      false,
      Constants.DEFAULT_DELETE_EXPIRED_STMT,
      "SIGNED",
      "SET innodb_lock_wait_timeout = ?"),

  MY_SQL_8(
      DialectFamily.MY_SQL,
      true,
      Constants.DEFAULT_DELETE_EXPIRED_STMT,
      "SIGNED",
      "SET innodb_lock_wait_timeout = ?"),

  H2(DialectFamily.H2, false, Constants.DEFAULT_DELETE_EXPIRED_STMT, "INT", "SET QUERY_TIMEOUT ?"),

  POSTGRESQL_9(
      DialectFamily.POSTGRESQL,
      true,
      "DELETE FROM {{table}} "
          + "WHERE id IN ("
          + "  SELECT id FROM {{table}} "
          + "  WHERE nextAttemptTime < ? AND "
          + "        processed = true AND "
          + "        blacklisted = false "
          + "  LIMIT ?)",
      "INTEGER",
      "SET LOCAL lock_timeout = '{{timeout}}s'"),

  @Deprecated
  POSTGRESQL__TEST_NO_SKIP_LOCK(
      DialectFamily.POSTGRESQL,
      false,
      "DELETE FROM {{table}} "
          + "WHERE id IN ("
          + "  SELECT id FROM {{table}} "
          + "  WHERE nextAttemptTime < ? AND "
          + "        processed = true AND "
          + "        blacklisted = false "
          + "  LIMIT ?)",
      "INTEGER",
      "SET LOCAL lock_timeout = '{{timeout}}s'");

  private final DialectFamily family;

  /**
   * @return True if hot row support ({@code SKIP LOCKED}) is available, increasing performance when
   *     there are multiple instances of the application potentially competing to process the same
   *     task.
   */
  @SuppressWarnings("JavaDoc")
  private final boolean supportsSkipLock;

  /** @return Format string for the SQL required to delete expired retained records. */
  @SuppressWarnings("JavaDoc")
  private final String deleteExpired;

  /** @return The type to use for a cast to integer. */
  @SuppressWarnings("JavaDoc")
  private final String integerCastType;

  /**
   * @return The command to use to set a query timeout. Not needed by JDBC but important for lower
   *     level protocols.
   */
  private final String queryTimeoutSetup;

  private static class Constants {
    static final String DEFAULT_DELETE_EXPIRED_STMT =
        "DELETE FROM {{table}} WHERE nextAttemptTime < ? AND processed = true AND blacklisted = false LIMIT ?";
  }
}
