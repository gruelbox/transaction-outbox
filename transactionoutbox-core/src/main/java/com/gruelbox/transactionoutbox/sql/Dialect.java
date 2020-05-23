package com.gruelbox.transactionoutbox.sql;

import com.gruelbox.transactionoutbox.Beta;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;

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
public abstract class Dialect extends com.gruelbox.transactionoutbox.Dialect {

  public static final Dialect MY_SQL_5 = new MySqlDialect(false);
  public static final Dialect MY_SQL_8 = new MySqlDialect(true);
  public static final Dialect H2 = new H2Dialect();
  public static final Dialect POSTGRESQL_9 = new PostgreSqlDialect(true);

  @Deprecated
  public static final Dialect POSTGRESQL__TEST_NO_SKIP_LOCK = new PostgreSqlDialect(false);

  public abstract Stream<Migration> migrations(String tableName);

  @Beta
  public String getDeleteExpired() {
    return "DELETE FROM {{table}} WHERE nextAttemptTime < ? AND processed = true AND blacklisted = false LIMIT ?";
  }

  /**
   * @return True if hot row support ({@code SKIP LOCKED}) is available, increasing performance when
   *     there are multiple instances of the application potentially competing to process the same
   *     task.
   */
  @Beta
  public abstract boolean isSupportsSkipLock();

  /** @return The type to use for a cast to integer. */
  @Beta
  public abstract String getIntegerCastType();

  /**
   * @return The command to use to set a query timeout. Not needed by JDBC but important for lower
   *     level protocols.
   */
  @Beta
  public abstract String getQueryTimeoutSetup();

  @Beta
  public String mapStatementToNative(String sql) {
    return sql;
  }

  @Beta
  public SqlResultRow mapResultFromNative(SqlResultRow row) {
    return row;
  }
}
