package com.gruelbox.transactionoutbox.acceptance.persistor;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gruelbox.transactionoutbox.Invocation;
import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.TransactionManager;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.testing.AbstractPersistorTest;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * MySQL-specific persistor acceptance tests.
 *
 * <p>The general tests migrate against an empty table, so they do not cover migration 14 converting
 * existing data. The migration test rebuilds the database at version 13 (where the MySQL {@code
 * blocked} column is a VARCHAR holding {@code "0"}/{@code "1"}), seeds records through the
 * persistor exactly as production did, then migrates to the latest version and asserts the column
 * type and stored values are converted correctly.
 */
abstract class AbstractMySqlPersistorTest extends AbstractPersistorTest {

  private static final int VERSION_BEFORE_BLOCKED_COLUMN_FIX = 13;

  @Test
  void migration14ConvertsExistingBlockedValues() throws Exception {
    Persistor persistor = persistor();
    TransactionManager txManager = txManager();
    MySqlMigrationTestSupport.recreateSchemaAtVersion(
        dialect(), txManager, VERSION_BEFORE_BLOCKED_COLUMN_FIX);

    Instant nextAttemptTime = Instant.now().minusSeconds(60).truncatedTo(MILLIS);
    TransactionOutboxEntry selectableEntry = entry("selectable", false, nextAttemptTime);
    TransactionOutboxEntry blockedEntry = entry("blocked", true, nextAttemptTime);
    txManager.inTransactionThrows(
        tx -> {
          persistor.save(tx, selectableEntry);
          persistor.save(tx, blockedEntry);
        });

    // The persistor writes booleans through setBoolean, so a VARCHAR column holds "0"/"1".
    assertEquals("varchar", blockedColumnType(txManager));
    assertEquals("0", blockedValue(txManager, selectableEntry.getId()));
    assertEquals("1", blockedValue(txManager, blockedEntry.getId()));

    // Migration 14 runs against the populated table.
    persistor.migrate(txManager);

    assertEquals("tinyint", blockedColumnType(txManager));
    // Boolean semantics survive the conversion: the blocked record is still excluded from the
    // batch, and unblocking it (which compares blocked = true) then includes it.
    assertEquals(Set.of(selectableEntry.getId()), selectableIds(persistor, txManager));
    boolean wasUnblocked =
        txManager.inTransactionReturnsThrows(tx -> persistor.unblock(tx, blockedEntry.getId()));
    assertTrue(wasUnblocked);
    assertEquals(
        Set.of(selectableEntry.getId(), blockedEntry.getId()), selectableIds(persistor, txManager));
  }

  private static TransactionOutboxEntry entry(String id, boolean blocked, Instant nextAttemptTime) {
    return TransactionOutboxEntry.builder()
        .id(id)
        .invocation(new Invocation("Foo", "Bar", new Class<?>[0], new Object[0]))
        .blocked(blocked)
        .nextAttemptTime(nextAttemptTime)
        .build();
  }

  private static Set<String> selectableIds(Persistor persistor, TransactionManager txManager)
      throws Exception {
    return txManager.inTransactionReturnsThrows(
        tx ->
            persistor.selectBatch(tx, 10, Instant.now()).stream()
                .map(TransactionOutboxEntry::getId)
                .collect(toSet()));
  }

  private static String blockedColumnType(TransactionManager txManager) throws Exception {
    return txManager.inTransactionReturnsThrows(
        tx -> {
          try (PreparedStatement stmt =
                  tx.connection()
                      .prepareStatement(
                          "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
                              + "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'TXNO_OUTBOX' "
                              + "AND COLUMN_NAME = 'blocked'");
              ResultSet rs = stmt.executeQuery()) {
            assertTrue(rs.next());
            return rs.getString(1);
          }
        });
  }

  private static String blockedValue(TransactionManager txManager, String id) throws Exception {
    return txManager.inTransactionReturnsThrows(
        tx -> {
          try (PreparedStatement stmt =
              tx.connection().prepareStatement("SELECT blocked FROM TXNO_OUTBOX WHERE id = ?")) {
            stmt.setString(1, id);
            try (ResultSet rs = stmt.executeQuery()) {
              assertTrue(rs.next());
              return rs.getString(1);
            }
          }
        });
  }
}
