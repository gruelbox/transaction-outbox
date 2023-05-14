package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.sql.Dialects;

/**
 * @deprecated Use {@link com.gruelbox.transactionoutbox.sql.Dialect}.
 */
@Deprecated
public abstract class Dialect extends com.gruelbox.transactionoutbox.sql.Dialect {
  public static final Dialect MY_SQL_5 = new DialectWrapper(Dialects.MY_SQL_5);
  public static final Dialect MY_SQL_8 = new DialectWrapper(Dialects.MY_SQL_8);
  public static final Dialect H2 = new DialectWrapper(Dialects.H2);
  public static final Dialect POSTGRESQL_9 = new DialectWrapper(Dialects.POSTGRESQL_9);
}
