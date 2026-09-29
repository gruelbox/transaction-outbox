package com.gruelbox.transactionoutbox.quarkus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.gruelbox.transactionoutbox.Transaction;
import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;
import jakarta.transaction.TransactionSynchronizationRegistry;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;

/**
 * {@link Transaction#prepareBatchStatement(String)} promises a statement that is cached and re-used
 * within a transaction, so that every row added to it is sent in one batch before the transaction
 * completes, and {@link Transaction#flushBatches()} sends what has been added so far.
 */
class QuarkusTransactionBatchStatementsTest {

  private static final String INSERT = "INSERT INTO OUTBOX (id) VALUES (?)";
  private static final String UPDATE = "UPDATE OUTBOX SET attempts = ? WHERE id = ?";

  private final List<String> calls = new ArrayList<>();
  private final Map<Object, Object> resources = new HashMap<>();
  private final List<Synchronization> synchronizations = new ArrayList<>();
  private final QuarkusTransactionManager transactionManager =
      new QuarkusTransactionManager(dataSource(), registry());

  @Test
  void shouldReuseOneStatementPerSqlWithinATransaction() {
    transactionManager.requireTransaction(
        tx -> assertSame(tx.prepareBatchStatement(INSERT), tx.prepareBatchStatement(INSERT)));

    assertEquals(List.of("prepare insert"), calls);
    assertEquals(1, synchronizations.size());
  }

  @Test
  void shouldSendEveryRowAddedInOneBatchBeforeCompletion() {
    transactionManager.requireTransaction(
        tx -> {
          addRow(tx, INSERT);
          addRow(tx, INSERT);
          addRow(tx, INSERT);
        });

    complete();

    assertEquals(
        List.of(
            "prepare insert",
            "insert addBatch",
            "insert addBatch",
            "insert addBatch",
            "insert executeBatch",
            "insert close"),
        calls);
  }

  @Test
  void shouldNotSendAgainAtCompletionWhenFlushedEarly() {
    transactionManager.requireTransaction(
        tx -> {
          addRow(tx, INSERT);
          tx.flushBatches();
          calls.add("flushed");
        });

    complete();

    assertEquals(
        List.of(
            "prepare insert", "insert addBatch", "insert executeBatch", "flushed", "insert close"),
        calls);
  }

  @Test
  void shouldSendRowsAddedAfterAFlushAtCompletion() {
    transactionManager.requireTransaction(
        tx -> {
          addRow(tx, INSERT);
          tx.flushBatches();
          calls.add("flushed");
          addRow(tx, INSERT);
          addRow(tx, INSERT);
        });

    complete();

    assertEquals(
        List.of(
            "prepare insert",
            "insert addBatch",
            "insert executeBatch",
            "flushed",
            "insert addBatch",
            "insert addBatch",
            "insert executeBatch",
            "insert close"),
        calls);
  }

  @Test
  void shouldSendNothingWhenNoRowWasAdded() {
    transactionManager.requireTransaction(
        tx -> {
          tx.prepareBatchStatement(INSERT);
          tx.flushBatches();
        });

    complete();

    assertEquals(List.of("prepare insert", "insert close"), calls);
  }

  @Test
  void shouldHaveNothingToFlushWhenNoStatementWasPrepared() {
    transactionManager.requireTransaction(Transaction::flushBatches);

    complete();

    assertEquals(List.of(), calls);
  }

  @Test
  void shouldKeepStatementsForDifferentSqlSeparate() {
    transactionManager.requireTransaction(
        tx -> {
          addRow(tx, INSERT);
          addRow(tx, UPDATE);
          addRow(tx, INSERT);
        });

    complete();

    assertEquals(2, calls.stream().filter(call -> call.startsWith("prepare")).count());
    assertEquals(1, java.util.Collections.frequency(calls, "insert executeBatch"));
    assertEquals(1, java.util.Collections.frequency(calls, "update executeBatch"));
  }

  @Test
  void shouldPrepareAFreshStatementInTheNextTransaction() {
    transactionManager.requireTransaction(tx -> addRow(tx, INSERT));
    complete();
    transactionManager.requireTransaction(tx -> addRow(tx, INSERT));
    complete();

    assertEquals(2, java.util.Collections.frequency(calls, "prepare insert"));
    assertEquals(2, java.util.Collections.frequency(calls, "insert executeBatch"));
  }

  private static void addRow(Transaction tx, String sql) {
    try {
      tx.prepareBatchStatement(sql).addBatch();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /** Completes the transaction as JTA does, then forgets everything tied to it. */
  private void complete() {
    for (var synchronization : List.copyOf(synchronizations)) {
      synchronization.beforeCompletion();
    }
    for (var synchronization : List.copyOf(synchronizations)) {
      synchronization.afterCompletion(Status.STATUS_COMMITTED);
    }
    synchronizations.clear();
    resources.clear();
  }

  private TransactionSynchronizationRegistry registry() {
    return (TransactionSynchronizationRegistry)
        Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] {TransactionSynchronizationRegistry.class},
            (proxy, method, args) ->
                switch (method.getName()) {
                  case "getTransactionStatus" -> Status.STATUS_ACTIVE;
                  case "putResource" -> {
                    resources.put(args[0], args[1]);
                    yield null;
                  }
                  case "getResource" -> resources.get(args[0]);
                  case "registerInterposedSynchronization" -> {
                    synchronizations.add((Synchronization) args[0]);
                    yield null;
                  }
                  default -> null;
                });
  }

  private DataSource dataSource() {
    return (DataSource)
        Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] {DataSource.class},
            (proxy, method, args) ->
                method.getName().equals("getConnection") ? connection() : null);
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
