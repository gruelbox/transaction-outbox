package com.gruelbox.transactionoutbox;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuperBuilder
@NoArgsConstructor(access = AccessLevel.PROTECTED)
abstract class BaseTransactionManager implements TransactionManager {

  private final ThreadLocal<Deque<List<Runnable>>> postCommitHooks = ThreadLocal.withInitial(LinkedList::new);
  private final ThreadLocal<Deque<Map<String, PreparedStatement>>> statements = ThreadLocal.withInitial(LinkedList::new);
  private final ThreadLocal<Deque<Connection>> currentConnection = ThreadLocal.withInitial(LinkedList::new);

  @Override
  public Connection getActiveConnection() {
    var conn = currentConnection.get().peek();
    if (conn == null) throw new NoTransactionActiveException();
    return conn;
  }

  @Override
  public final void inTransaction(Runnable runnable) {
    TransactionManager.super.inTransaction(runnable);
  }

  @Override
  public final <T> T inTransactionReturns(Supplier<T> supplier) {
    return TransactionManager.super.inTransactionReturns(supplier);
  }

  @Override
  public final void inTransactionThrows(ThrowingRunnable runnable) throws Exception {
    TransactionManager.super.inTransactionThrows(runnable);
  }

  void pushLists() {
    postCommitHooks.get().push(new ArrayList<>());
    statements.get().push(new HashMap<>());
  }

  void popLists() {
    postCommitHooks.get().pop();
    if (postCommitHooks.get().isEmpty())
      postCommitHooks.remove();
    statements.get().pop();
    if (statements.get().isEmpty())
      postCommitHooks.remove();
  }

  void pushConnection(Connection connection) {
    currentConnection.get().push(connection);
  }

  void popConnection() {
    currentConnection.get().pop();
    if (currentConnection.get().isEmpty()) {
      currentConnection.remove();
    }
  }

  Connection peekConnection() {
    return currentConnection.get().peek();
  }

  void processHooks() {
    var hooks = postCommitHooks.get().peek();
    if (!hooks.isEmpty()) {
      log.debug("Running post-commit hooks");
      hooks.forEach(Runnable::run);
    }
  }

  @Override
  public final void addPostCommitHook(Runnable runnable) {
    postCommitHooks.get().peek().add(runnable);
  }

  @Override
  public final PreparedStatement prepareBatchStatement(String sql) {
    return statements
        .get()
        .peek()
        .computeIfAbsent(
            sql, s -> Utils.uncheckedly(() -> getActiveConnection().prepareStatement(s)));
  }

  void flushBatches() {
    var smts = statements.get().peek();
    if (!smts.isEmpty()) {
      log.debug("Flushing batches");
      smts.values().forEach(it -> Utils.uncheck(it::executeBatch));
    }
  }

  void closeBatchStatements() {
    var smts = statements.get().peek();
    if (!smts.isEmpty()) {
      log.debug("Closing batch statements");
      Utils.safelyClose(smts.values());
    }
  }
}
