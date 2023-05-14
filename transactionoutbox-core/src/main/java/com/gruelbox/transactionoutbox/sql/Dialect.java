package com.gruelbox.transactionoutbox.sql;

import com.gruelbox.transactionoutbox.Beta;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;

/**
 * Encapsulates the variations of SQL dialects used on supported RDBMSes. This is not a general
 * purpose dialect, such as that used by jOOQ. It purely serves the purpose of mapping the features
 * needed by {@link com.gruelbox.transactionoutbox.TransactionOutbox}.
 *
 * <p>Note that all properties of the dialect are very much under iteration; not much design has
 * gone into them; they've only need added as necessary to make things work. At some point this will
 * be rationalised, but in the meantime extending this class should be considered highly unstable.
 */
@AllArgsConstructor
public abstract class Dialect {

  /**
   * The database migrations required to bring any database with no transaction outbox schema
   * elements up to the latest version on this database dialect.
   *
   * @param tableName The main outbox table name.
   * @return The migrations.
   */
  @Beta
  public abstract Stream<SqlMigration> migrations(String tableName);

  /**
   * @return True if hot row support ({@code SKIP LOCKED}) is available, increasing performance when
   *     there are multiple instances of the application potentially competing to process the same
   *     task.
   */
  @Beta
  public abstract boolean isSupportsSkipLock();

  /**
   * @return The statement required to delete a limited-size batch of processed, non-blocked records
   *     which have passed their expiry date.
   */
  @Beta
  public String getDeleteExpired() {
    return "DELETE FROM {{table}} WHERE nextAttemptTime < ? AND processed = true AND blocked = false LIMIT ?";
  }

  /**
   * @return The type to use for a cast to integer.
   */
  @Beta
  public abstract String getIntegerCastType();

  /**
   * @return The command to use to set a query timeout. Not needed by JDBC but important for lower
   *     level protocols.
   */
  @Beta
  public abstract String getQueryTimeoutSetup();

  /**
   * @return The SQL snippet to use to limit query results to a certain size.
   */
  public String getLimitCriteria() {
    return " LIMIT ?";
  }

  /**
   * @return Connection check SQL.
   */
  public String getConnectionCheck() {
    return "SELECT 1";
  }

  /**
   * @param criteriaValue A boolean.
   * @return The SQL representation.
   */
  public String booleanValue(boolean criteriaValue) {
    return criteriaValue ? Boolean.TRUE.toString() : Boolean.FALSE.toString();
  }

  /**
   * Performs string conversion to convert a statement accepted by JDBC to the native format of the
   * database. Only called if the {@link com.gruelbox.transactionoutbox.Persistor} uses a data
   * access API which doesn't support JDBC conventions.. This mainly exists for PostgreSQL, which
   * has a lot of complicated logic in the JDBC driver which converts things like {@code ?, ?} for
   * parameters to the native {@code $1, $2...}
   *
   * @param sql The SQL to convert.
   * @return The converted SQL.
   */
  @Beta
  public String mapStatementToNative(String sql) {
    return sql;
  }

  /**
   * Allows interception and type conversion of result sets on RDBMSes that don't support the types
   * that {@link com.gruelbox.transactionoutbox.TransactionOutbox} uses.
   *
   * @param row Access to the raw row data.
   * @return A wrapper.
   */
  @Beta
  public SqlResultRow mapResultFromNative(SqlResultRow row) {
    return row;
  }

  public CompletableFuture<Integer> createVersionTableIfNotExists(
      Function<String, CompletableFuture<Integer>> statementInvoker) {
    return statementInvoker.apply(
        "CREATE TABLE IF NOT EXISTS TXNO_VERSION AS SELECT CAST(0 AS "
            + getIntegerCastType()
            + ") AS version");
  }
}
