package com.gruelbox.transactionoutbox;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.gruelbox.transactionoutbox.acceptance.TestUtils;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

class TestDefaultPersistorConfiguration {
  @Test
  final void whenMigrateIsFalseDoNotMigrate() throws Exception {
    TransactionManager transactionManager = simpleTxnManager();
    TestUtils.runSql(transactionManager, "DROP ALL OBJECTS");

    TransactionOutbox outbox =
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

  private TransactionManager simpleTxnManager() {
    return TransactionManager.fromConnectionDetails(
        "org.h2.Driver",
        "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;DEFAULT_LOCK_TIMEOUT=60000;LOB_TIMEOUT=2000;MV_STORE=TRUE",
        "test",
        "test");
  }
}
