package com.gruelbox.transactionoutbox.sql;

import com.gruelbox.transactionoutbox.Beta;
import lombok.Value;

/**
 * Describes an audited SQL statement that forms part of a linear series of database schema
 * migrations.
 */
@Value
@Beta
public final class SqlMigration {
  int version;
  String name;
  String sql;
}
