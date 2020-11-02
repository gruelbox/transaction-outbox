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
  MY_SQL_5(false, Constants.DEFAULT_DELETE_EXPIRED_STMT),
  MY_SQL_8(true, Constants.DEFAULT_DELETE_EXPIRED_STMT),
  POSTGRESQL_9(
      true,
      "DELETE FROM {{table}} WHERE ctid IN (SELECT ctid FROM {{table}} WHERE nextAttemptTime < ? AND processed = true AND blocked = false LIMIT ?)"),
  H2(false, Constants.DEFAULT_DELETE_EXPIRED_STMT);

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

  private static class Constants {
    static final String DEFAULT_DELETE_EXPIRED_STMT =
        "DELETE FROM {{table}} WHERE nextAttemptTime < ? AND processed = true AND blocked = false LIMIT ?";
  }
}
