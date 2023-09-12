package com.gruelbox.transactionoutbox;

import lombok.EqualsAndHashCode;

import java.sql.SQLException;
import java.sql.Statement;

/** The SQL dialects supported by {@link DefaultPersistor}. */
public interface Dialect {
  Dialect MY_SQL_5 = new DialectMySQL5Impl();
  Dialect MY_SQL_8 = new DialectMySQL8Impl();
  Dialect POSTGRESQL_9 = new DialectPostgres9Impl();
  Dialect H2 = new DialectH2Impl();
  Dialect ORACLE = new DialectOracleImpl();

  String lock(String tableName);

  String unblock(String tableName);

  String selectBatch(String tableName, String allFields, int batchSize);

  String deleteExpired(String tableName, int batchSize);

  boolean isSupportsSkipLock();

  void createVersionTableIfNotExists(Statement s) throws SQLException;
  // Required so the dialects can be used as keys in a Map.
  boolean equals(Object o);
  int hashCode();
}
