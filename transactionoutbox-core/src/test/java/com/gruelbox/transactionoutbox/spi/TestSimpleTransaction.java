package com.gruelbox.transactionoutbox.spi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Flushing sends what has been queued and no more, so that flushing early does not repeat itself
 * when the transaction flushes again before the commit.
 */
class TestSimpleTransaction {

  private static final String INSERT = "INSERT INTO OUTBOX (id) VALUES (?)";
  private static final String UPDATE = "UPDATE OUTBOX SET attempts = ? WHERE id = ?";

  private final List<String> calls = new ArrayList<>();
  private final SimpleTransaction transaction = new SimpleTransaction(connection(), null);

  @Test
  void shouldReuseOneStatementPerSql() {
    assertSame(
        transaction.prepareBatchStatement(INSERT), transaction.prepareBatchStatement(INSERT));

    assertEquals(List.of("prepare insert"), calls);
  }

  @Test
  void shouldSendEveryRowAddedInOneBatch() throws SQLException {
    addRow(INSERT);
    addRow(INSERT);
    addRow(INSERT);

    transaction.flushBatches();

    assertEquals(
        List.of(
            "prepare insert",
            "insert addBatch",
            "insert addBatch",
            "insert addBatch",
            "insert executeBatch"),
        calls);
  }

  @Test
  void shouldNotSendAgainWhenFlushedTwice() throws SQLException {
    addRow(INSERT);

    transaction.flushBatches();
    transaction.flushBatches();

    assertEquals(List.of("prepare insert", "insert addBatch", "insert executeBatch"), calls);
  }

  @Test
  void shouldSendRowsAddedAfterAFlushOnTheNextFlush() throws SQLException {
    addRow(INSERT);
    transaction.flushBatches();
    addRow(INSERT);
    addRow(INSERT);

    transaction.flushBatches();

    assertEquals(
        List.of(
            "prepare insert",
            "insert addBatch",
            "insert executeBatch",
            "insert addBatch",
            "insert addBatch",
            "insert executeBatch"),
        calls);
  }

  @Test
  void shouldSendNothingWhenNoRowWasAdded() {
    transaction.prepareBatchStatement(INSERT);

    transaction.flushBatches();

    assertEquals(List.of("prepare insert"), calls);
  }

  @Test
  void shouldSendOnlyTheStatementsThatHaveRows() throws SQLException {
    addRow(INSERT);
    addRow(UPDATE);
    transaction.flushBatches();
    addRow(INSERT);

    transaction.flushBatches();

    assertEquals(2, Collections.frequency(calls, "insert executeBatch"));
    assertEquals(1, Collections.frequency(calls, "update executeBatch"));
  }

  @Test
  void shouldCloseTheStatements() throws SQLException {
    addRow(INSERT);

    transaction.close();

    assertEquals(List.of("prepare insert", "insert addBatch", "insert close"), calls);
  }

  private void addRow(String sql) throws SQLException {
    transaction.prepareBatchStatement(sql).addBatch();
  }

  private Connection connection() {
    return (Connection)
        Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] {Connection.class},
            (proxy, method, args) -> {
              if (!method.getName().equals("prepareStatement")) {
                return null;
              }
              var label = ((String) args[0]).startsWith("INSERT") ? "insert" : "update";
              calls.add("prepare " + label);
              return statement(label);
            });
  }

  private PreparedStatement statement(String label) {
    return (PreparedStatement)
        Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] {PreparedStatement.class},
            (proxy, method, args) -> {
              if (method.getDeclaringClass() == Object.class) {
                return switch (method.getName()) {
                  case "hashCode" -> System.identityHashCode(proxy);
                  case "equals" -> proxy == args[0];
                  default -> label;
                };
              }
              calls.add(label + " " + method.getName());
              return method.getReturnType() == int[].class ? new int[0] : null;
            });
  }
}
