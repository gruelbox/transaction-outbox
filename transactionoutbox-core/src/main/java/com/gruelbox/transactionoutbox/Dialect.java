package com.gruelbox.transactionoutbox;

/** @deprecated Use {@link com.gruelbox.transactionoutbox.sql.Dialect}. */
@Deprecated
public abstract class Dialect extends com.gruelbox.transactionoutbox.sql.Dialect {
  public static final Dialect MY_SQL_5 =
      new DialectWrapper(com.gruelbox.transactionoutbox.sql.Dialect.MY_SQL_5);
  public static final Dialect MY_SQL_8 =
      new DialectWrapper(com.gruelbox.transactionoutbox.sql.Dialect.MY_SQL_8);
  public static final Dialect H2 =
      new DialectWrapper(com.gruelbox.transactionoutbox.sql.Dialect.H2);
  public static final Dialect POSTGRESQL_9 =
      new DialectWrapper(com.gruelbox.transactionoutbox.sql.Dialect.POSTGRESQL_9);
}
