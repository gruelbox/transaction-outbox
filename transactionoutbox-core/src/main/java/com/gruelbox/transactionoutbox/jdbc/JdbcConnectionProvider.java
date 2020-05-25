package com.gruelbox.transactionoutbox.jdbc;

import java.sql.Connection;

/** Source for JDBC connections to be provided to a {@link SimpleTransactionManager}. */
public interface JdbcConnectionProvider {

  /**
   * Requests a new connection, or an available connection from a pool. The caller is responsible
   * for calling {@link Connection#close()}.
   *
   * @return The connection.
   */
  Connection obtainConnection();
}
