package com.gruelbox.transactionoutbox;

import lombok.extern.slf4j.Slf4j;

import java.io.PrintWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.gruelbox.transactionoutbox.spi.Utils.uncheck;

/**
 * Simple database migration manager. Inspired by Flyway, Liquibase, Morf etc, just trimmed down for
 * minimum dependencies.
 */
@Slf4j
class DefaultMigrationManager {

  private static final Executor basicExecutor =
      runnable -> {
        new Thread(runnable).start();
      };

  private static CountDownLatch waitLatch;
  private static CountDownLatch readyLatch;

  static void withLatch(CountDownLatch readyLatch, Consumer<CountDownLatch> runnable) {
    waitLatch = new CountDownLatch(1);
    DefaultMigrationManager.readyLatch = readyLatch;
    try {
      runnable.accept(waitLatch);
    } finally {
      waitLatch = null;
      DefaultMigrationManager.readyLatch = null;
    }
  }

  static void migrate(TransactionManager transactionManager, Dialect dialect) {
    transactionManager.inTransaction(
        transaction -> {
          try {
            int currentVersion = currentVersion(transaction.connection(), dialect);
            dialect
                .getMigrations()
                .filter(migration -> migration.getVersion() > currentVersion)
                .forEach(
                    migration ->
                        uncheck(
                            () -> runSql(transactionManager, transaction.connection(), migration)));
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
                printWriter.println("-- Nothing for " + dialect.getName());
              } else {
                printWriter.println(migration.getSql());
              }
              printWriter.println();
            });
    printWriter.flush();
  }

  private static void runSql(TransactionManager txm, Connection connection, Migration migration)
      throws SQLException {
    log.info("Running migration {}: {}", migration.getVersion(), migration.getName());

    if (migration.getSql() != null && !migration.getSql().isEmpty()) {
      CompletableFuture.runAsync(
              () -> {
                try {
                  txm.inTransactionThrows(
                      tx -> {
                        try (var s = tx.connection().prepareStatement(migration.getSql())) {
                          s.execute();
                        }
                      });
                } catch (SQLException e) {
                  throw new RuntimeException(e);
                }
              },
              basicExecutor)
          .join();
    }

    try (var s = connection.prepareStatement("UPDATE TXNO_VERSION SET version = ?")) {
      s.setInt(1, migration.getVersion());
      if (s.executeUpdate() != 1) {
        throw new IllegalStateException("Version table should already exist");
      }
    }
  }

  private static int currentVersion(Connection connection, Dialect dialect) throws SQLException {
    dialect.createVersionTableIfNotExists(connection);
    int version = fetchCurrentVersion(connection, dialect);
    if (version >= 0) {
      return version;
    }
    try {
      log.info("No version record found. Attempting to create");
      if (waitLatch != null) {
        log.info("Stopping at latch");
        readyLatch.countDown();
        if (!waitLatch.await(10, TimeUnit.SECONDS)) {
          throw new IllegalStateException("Latch not released in 10 seconds");
        }
        log.info("Latch released");
      }
      try (var s = connection.prepareStatement("INSERT INTO TXNO_VERSION (version) VALUES (0)")) {
        s.executeUpdate();
      }
      log.info("Created version record.");
      return fetchCurrentVersion(connection, dialect);
    } catch (Exception e) {
      log.info(
          "Error attempting to create ({} - {}). May have been beaten to it, attempting second fetch",
          e.getClass().getSimpleName(),
          e.getMessage());
      version = fetchCurrentVersion(connection, dialect);
      if (version >= 0) {
        return version;
      }
      throw new IllegalStateException("Unable to read or create version record", e);
    }
  }

  private static int fetchCurrentVersion(Connection connection, Dialect dialect)
      throws SQLException {
    try (PreparedStatement s = connection.prepareStatement(dialect.getFetchCurrentVersion());
        ResultSet rs = s.executeQuery()) {
      if (rs.next()) {
        var version = rs.getInt(1);
        log.info("Current version is {}, obtained lock", version);
        if (rs.next()) {
          throw new IllegalStateException("More than one version record");
        }
        return version;
      }
      return -1;
    }
  }
}
