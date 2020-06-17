package com.gruelbox.transactionoutbox.sql;

/** Convenience access to {@link Dialect}s. */
public final class Dialects {

  private Dialects() {}

  /** Supports MySQL 5.6 and 5.7. */
  public static final Dialect MY_SQL_5 = new MySqlDialect(false);

  /** Supports MySQL 8. */
  public static final Dialect MY_SQL_8 = new MySqlDialect(true);

  /** Supports H2. */
  public static final Dialect H2 = new H2Dialect();

  /** Supports PostgreSQL 9, 10, 11 and 12. */
  public static final Dialect POSTGRESQL_9 = new PostgreSqlDialect(true);
}
