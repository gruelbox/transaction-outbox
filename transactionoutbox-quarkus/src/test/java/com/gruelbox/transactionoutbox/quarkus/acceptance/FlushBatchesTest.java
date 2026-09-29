package com.gruelbox.transactionoutbox.quarkus.acceptance;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.gruelbox.transactionoutbox.Transaction;
import com.gruelbox.transactionoutbox.quarkus.QuarkusTransactionManager;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
class FlushBatchesTest {

  private static final String INSERT = "insert into toto values (?)";

  @Inject QuarkusTransactionManager transactionManager;
  @Inject DaoImpl dao;

  @BeforeEach
  void purgeDatabase() {
    dao.purge();
  }

  /** The transaction can see what a flush sent before it completes, and completing adds nothing. */
  @Test
  void flushBatches_sendsQueuedRowsOnceAndBeforeCompletion() {
    var visibleBeforeCompletion = new AtomicInteger();

    transactionManager.inTransaction(
        tx -> {
          addRow(tx, "a");
          addRow(tx, "b");
          tx.flushBatches();
          visibleBeforeCompletion.set(countRows(tx));
        });

    assertEquals(2, visibleBeforeCompletion.get());
    assertEquals(2, dao.getFromDatabase().size());
  }

  @Test
  void completion_sendsRowsAddedAfterAFlush() {
    transactionManager.inTransaction(
        tx -> {
          addRow(tx, "a");
          tx.flushBatches();
          addRow(tx, "b");
        });

    assertEquals(2, dao.getFromDatabase().size());
  }

  private static void addRow(Transaction tx, String value) {
    try {
      var statement = tx.prepareBatchStatement(INSERT);
      statement.setString(1, value);
      statement.addBatch();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private static int countRows(Transaction tx) {
    try (var statement = tx.connection().createStatement();
        var rows = statement.executeQuery("select count(*) from toto")) {
      rows.next();
      return rows.getInt(1);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
