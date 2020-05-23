package com.gruelbox.transactionoutbox;

@Deprecated
public abstract class Dialect {

  public static final Dialect MY_SQL_5 = com.gruelbox.transactionoutbox.sql.Dialect.MY_SQL_5;
  public static final Dialect MY_SQL_8 = com.gruelbox.transactionoutbox.sql.Dialect.MY_SQL_8;
  public static final Dialect H2 = com.gruelbox.transactionoutbox.sql.Dialect.H2;
  public static final Dialect POSTGRESQL_9 =
      com.gruelbox.transactionoutbox.sql.Dialect.POSTGRESQL_9;
}
