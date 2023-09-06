package com.gruelbox.transactionoutbox;

import lombok.AllArgsConstructor;
import lombok.Getter;

/** The SQL dialects supported by {@link DefaultPersistor}. */
@AllArgsConstructor
@Getter
@Beta
public enum Dialect {
  MY_SQL_5, //
  MY_SQL_8, //
  POSTGRESQL_9, //
  H2, //
  ORACLE
}
