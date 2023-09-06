package com.gruelbox.transactionoutbox;

/** Dialect SQL implementation for H2. */
public class DialectSqlH2Impl extends DialectSqlMySQL5Impl {
  @Override
  public Dialect getDialect() {
    return Dialect.H2;
  }
}
