package com.gruelbox.transactionoutbox;

import java.sql.SQLException;
import java.sql.Statement;
import lombok.EqualsAndHashCode;

/** Dialect SQL implementation for Oracle. */
@EqualsAndHashCode
public class DialectOracleImpl extends DialectMySQL8Impl {

  @Override
  public String selectBatch(String tableName, String allFields, int batchSize) {
    return "SELECT "
        + allFields
        + " FROM "
        + tableName
        + " WHERE nextAttemptTime < ? AND blocked = ? AND processed = ? "
        + " AND ROWNUM <= "
        + batchSize
        + " FOR UPDATE SKIP LOCKED";
  }

  @Override
  public String deleteExpired(String tableName, int batchSize) {
    return "DELETE FROM "
        + tableName
        + " WHERE nextAttemptTime < ? AND processed = ? AND blocked = ?"
        + " AND ROWNUM <= "
        + batchSize;
  }

  @Override
  public void createVersionTableIfNotExists(Statement s) throws SQLException {
    try {
      s.execute("CREATE TABLE TXNO_VERSION (version NUMBER)");
    } catch (SQLException e) {
      // oracle code for name already used by an existing object
      if (!e.getMessage().contains("955")) {
        throw e;
      }
    }
  }
}
