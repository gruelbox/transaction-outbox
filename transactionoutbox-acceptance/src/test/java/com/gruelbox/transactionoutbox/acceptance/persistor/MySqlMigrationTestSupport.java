package com.gruelbox.transactionoutbox.acceptance.persistor;

import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.Migration;
import com.gruelbox.transactionoutbox.TransactionManager;
import com.gruelbox.transactionoutbox.testing.TestUtils;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/** Test support for rebuilding a MySQL database at a historical migration version. */
final class MySqlMigrationTestSupport {

  private MySqlMigrationTestSupport() {}

  static void recreateSchemaAtVersion(
      Dialect dialect, TransactionManager txManager, int targetVersion) throws Exception {
    // Start empty so migrations after targetVersion are not already present.
    dropAllDatabaseObjects(txManager);
    txManager.inTransactionThrows(
        tx -> {
          dialect.createVersionTableIfNotExists(tx.connection());
          try (PreparedStatement stmt =
              tx.connection().prepareStatement("INSERT INTO TXNO_VERSION (version) VALUES (0)")) {
            stmt.executeUpdate();
          }
        });

    for (Migration migration :
        dialect
            .getMigrations()
            .filter(candidate -> candidate.getVersion() <= targetVersion)
            .toList()) {
      if (migration.getSql() != null && !migration.getSql().isEmpty()) {
        TestUtils.runSql(txManager, migration.getSql());
      }
      TestUtils.runSql(txManager, "UPDATE TXNO_VERSION SET version = " + migration.getVersion());
    }
  }

  private static void dropAllDatabaseObjects(TransactionManager txManager) throws Exception {
    txManager.inTransactionThrows(
        tx -> {
          List<String> tables = new ArrayList<>();
          List<String> views = new ArrayList<>();
          try (PreparedStatement stmt =
                  tx.connection()
                      .prepareStatement(
                          "SELECT TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES "
                              + "WHERE TABLE_SCHEMA = DATABASE()");
              ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
              ("VIEW".equals(rs.getString(2)) ? views : tables).add(rs.getString(1));
            }
          }

          try (Statement stmt = tx.connection().createStatement()) {
            for (String view : views) {
              stmt.execute("DROP VIEW " + quotedIdentifier(view));
            }
            stmt.execute("SET FOREIGN_KEY_CHECKS = 0");
            try {
              for (String table : tables) {
                stmt.execute("DROP TABLE " + quotedIdentifier(table));
              }
            } finally {
              stmt.execute("SET FOREIGN_KEY_CHECKS = 1");
            }
          }
        });
  }

  private static String quotedIdentifier(String identifier) {
    return "`" + identifier.replace("`", "``") + "`";
  }
}
