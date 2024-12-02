package com.gruelbox.transactionoutbox;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.stream.Stream;

/** The SQL dialects supported by {@link DefaultPersistor}. */
public interface Dialect {
  String getName();

  String getDelete();

  /**
   * @return Format string for the SQL required to delete expired retained records.
   */
  String getDeleteExpired();

  String getSelectBatch();

  String getLock();

  String getCheckSql();

  String getFetchNextInAllTopics();

  String getFetchCurrentVersion();

  String getFetchNextSequence();

  String booleanValue(boolean criteriaValue);

  void createVersionTableIfNotExists(Connection connection) throws SQLException;

  Stream<Migration> getMigrations();

  Dialect MY_SQL_5 = new MySQL5Dialect();
  Dialect MY_SQL_8 = new MySQL8Dialect();
  Dialect POSTGRESQL_9 = new Postgresql9Dialect();
  Dialect H2 = new H2Dialect();
  Dialect ORACLE = new OracleDialect();
  Dialect MS_SQL_SERVER = new MsSQLServerDialect();
}
