package com.gruelbox.transactionoutbox.spi;

import static com.gruelbox.transactionoutbox.spi.Utils.safelyClose;
import static com.gruelbox.transactionoutbox.spi.Utils.uncheck;

import com.gruelbox.transactionoutbox.Transaction;
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
@AllArgsConstructor(access = AccessLevel.PUBLIC)
public final class SimpleTransaction implements Transaction, AutoCloseable {

  private final List<Runnable> postCommitHooks = new ArrayList<>();
  private final Map<String, PreparedStatement> preparedStatements = new HashMap<>();
  private final Connection connection;
  private final Object context;

  @Override
  public Connection connection() {
    return connection;
  }

  @Override
  public void addPostCommitHook(Runnable runnable) {
    postCommitHooks.add(runnable);
  }

  @Override
  public PreparedStatement prepareBatchStatement(String sql) {
    return preparedStatements.computeIfAbsent(
        sql, s -> Utils.uncheckedly(() -> connection.prepareStatement(s)));
  }

  public void flushBatches() {
    if (!preparedStatements.isEmpty()) {
      log.debug("Flushing batches");
      for (PreparedStatement statement : preparedStatements.values()) {
        uncheck(statement::executeBatch);
      }
    }
  }

  public void processHooks() {
    if (!postCommitHooks.isEmpty()) {
      log.debug("Running post-commit hooks");
      postCommitHooks.forEach(Runnable::run);
    }
  }

  public void commit() {
    uncheck(connection::commit);
  }

  public void rollback() throws SQLException {
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
