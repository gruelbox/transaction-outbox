package com.gruelbox.transactionoutbox;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.io.StringWriter;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

class TestDefaultPersistorConfiguration {
  @Test
  final void whenMigrateIsFalseDoNotMigrate() throws Exception {
    TransactionManager transactionManager = simpleTxnManager();
    runSql(transactionManager, "DROP ALL OBJECTS");

    TransactionOutbox.builder()
        .transactionManager(transactionManager)
        .persistor(DefaultPersistor.builder().dialect(Dialect.H2).migrate(false).build())
        .build();

    transactionManager.inTransactionThrows(
        tx -> {
          try (Statement statement = tx.connection().createStatement()) {
            try (ResultSet rs =
                statement.executeQuery(
                    "SELECT COUNT(*)"
                        + " FROM INFORMATION_SCHEMA.TABLES"
                        + " WHERE TABLE_NAME IN ('TXNO_OUTBOX', 'TXNO_VERSION')")) {
              rs.next();
              assertThat(rs.getInt(1), is(0));
            }
          }
        });
  }

  @Test
  final void writeSchema() {
    StringWriter stringWriter = new StringWriter();

    DefaultPersistor defaultPersistor = DefaultPersistor.builder().dialect(Dialect.H2).build();
    defaultPersistor.writeSchema(stringWriter);

    String migrations = stringWriter.toString();

    assertThat(migrations, startsWith("-- 1: Create outbox table"));
    assertThat(
        migrations,
        containsString(
            "-- 2: Add unique request id"
                + System.lineSeparator()
                + "ALTER TABLE TXNO_OUTBOX ADD COLUMN uniqueRequestId VARCHAR(100) NULL UNIQUE"));
    assertThat(
        migrations,
        containsString(
            "-- 8: Update length of invocation column on outbox for MySQL dialects only."
                + System.lineSeparator()
                + "-- Nothing for H2"));
  }

  private TransactionManager simpleTxnManager() {
    return TransactionManager.fromConnectionDetails(
        "org.h2.Driver",
        "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;DEFAULT_LOCK_TIMEOUT=60000;LOB_TIMEOUT=2000;MV_STORE=TRUE",
        "test",
        "test");
  }

  private void runSql(
      TransactionManager transactionManager, @SuppressWarnings("SameParameterValue") String sql) {
    transactionManager.inTransaction(
        tx -> {
          try {
            try (Statement statement = tx.connection().createStatement()) {
              statement.execute(sql);
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }
}
