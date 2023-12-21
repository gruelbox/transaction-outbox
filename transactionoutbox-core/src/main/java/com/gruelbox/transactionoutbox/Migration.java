package com.gruelbox.transactionoutbox;

import lombok.Value;

/**
 * A database migration script entry. See {@link Dialect#getMigrations()}.
 */
@Value
public class Migration {
  int version;
  String name;
  String sql;

  public Migration withSql(String sql) {
    return new Migration(version, name, sql);
  }
}
