package com.gruelbox.transactionoutbox;

import static com.gruelbox.transactionoutbox.Utils.safelyClose;
import static com.gruelbox.transactionoutbox.Utils.uncheck;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PROTECTED)
class SimpleTransaction implements Transaction, AutoCloseable {

  private final List<Runnable> postCommitHooks = new ArrayList<>();
  private final Map<String, PreparedStatement> preparedStatements = new HashMap<>();
  private final Connection connection;
  private final Object context;

  @Override
  public final Connection connection() {
    return connection;
  }

  @Override
  public final void addPostCommitHook(Runnable runnable) {
    postCommitHooks.add(runnable);
  }

  @Override
  public final PreparedStatement prepareBatchStatement(String sql) {
    return preparedStatements.computeIfAbsent(
        sql, s -> Utils.uncheckedly(() -> connection.prepareStatement(s)));
  }

  final void flushBatches() {
    if (!preparedStatements.isEmpty()) {
      log.debug("Flushing batches");
      for (PreparedStatement statement : preparedStatements.values()) {
        uncheck(statement::executeBatch);
      }
    }
  }

  final void processHooks() {
    if (!postCommitHooks.isEmpty()) {
      log.debug("Running post-commit hooks");
      postCommitHooks.forEach(Runnable::run);
    }
  }

  void commit() {
    uncheck(connection::commit);
  }

  void rollback() throws SQLException {
    connection.rollback();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T context() {
    return (T) context;
  }

  @Override
  public void close() {
    if (!preparedStatements.isEmpty()) {
      log.debug("Closing batch statements");
      safelyClose(preparedStatements.values());
    }
  }
}
