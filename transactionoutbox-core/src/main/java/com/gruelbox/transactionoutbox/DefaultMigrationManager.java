package com.gruelbox.transactionoutbox;

import static com.gruelbox.transactionoutbox.spi.Utils.uncheck;

import java.io.PrintWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import lombok.extern.slf4j.Slf4j;

/**
 * Simple database migration manager. Inspired by Flyway, Liquibase, Morf etc, just trimmed down for
 * minimum dependencies.
 */
@Slf4j
class DefaultMigrationManager {

  static void migrate(TransactionManager transactionManager, Dialect dialect) {
    transactionManager.inTransaction(
        transaction -> {
          try {
            int currentVersion = currentVersion(transaction.connection(), dialect);
            dialect
                .getMigrations()
                .filter(migration -> migration.getVersion() > currentVersion)
                .forEach(migration -> uncheck(() -> runSql(transaction.connection(), migration)));
          } catch (Exception e) {
            throw new RuntimeException("Migrations failed", e);
          }
        });
  }

  static void writeSchema(Writer writer, Dialect dialect) {
    PrintWriter printWriter = new PrintWriter(writer);
    dialect
        .getMigrations()
        .forEach(
            migration -> {
              printWriter.print("-- ");
              printWriter.print(migration.getVersion());
              printWriter.print(": ");
              printWriter.println(migration.getName());
              if (migration.getSql() == null || migration.getSql().isEmpty()) {
                printWriter.println("-- Nothing for " + dialect);
              } else {
                printWriter.println(migration.getSql());
              }
              printWriter.println();
            });
    printWriter.flush();
  }

  private static void runSql(Connection connection, Migration migration) throws SQLException {
    log.info("Running migration: {}", migration.getName());
    try (Statement s = connection.createStatement()) {
      if (migration.getSql() != null && !migration.getSql().isEmpty()) {
        s.execute(migration.getSql());
      }
      if (s.executeUpdate("UPDATE TXNO_VERSION SET version = " + migration.getVersion()) != 1) {
        // TODO shouldn't be necessary if the lock is done correctly
        s.execute("INSERT INTO TXNO_VERSION VALUES (" + migration.getVersion() + ")");
      }
    }
  }

  private static int currentVersion(Connection connection, Dialect dialect) throws SQLException {
    dialect.createVersionTableIfNotExists(connection);
    try (Statement s = connection.createStatement();
        ResultSet rs = s.executeQuery("SELECT version FROM TXNO_VERSION FOR UPDATE")) {
      if (!rs.next()) {
        // TODO should attempt to "win" at creating the record and then lock it
        return 0;
      }
      return rs.getInt(1);
    }
  }
}
