package com.gruelbox.transactionoutbox.jdbc;

import static com.gruelbox.transactionoutbox.Utils.safelyClose;
import static com.gruelbox.transactionoutbox.Utils.uncheck;
import static com.gruelbox.transactionoutbox.Utils.uncheckedly;

import com.gruelbox.transactionoutbox.Beta;
import com.gruelbox.transactionoutbox.Utils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Beta
@Slf4j
@AllArgsConstructor
public class SimpleTransaction<CONTEXT> implements JdbcTransaction, AutoCloseable {

  private final List<Supplier<CompletableFuture<Void>>> postCommitHooks = new ArrayList<>();
  private final Map<String, PreparedStatement> preparedStatements = new HashMap<>();
  private final Connection connection;
  private final CONTEXT context;

  @Override
  public final Connection connection() {
    return connection;
  }

  @Override
  public void addPostCommitHook(Supplier<CompletableFuture<Void>> hook) {
    postCommitHooks.add(hook);
  }

  @Override
  public final PreparedStatement prepareBatchStatement(String sql) {
    return preparedStatements.computeIfAbsent(
        sql, s -> uncheckedly(() -> connection.prepareStatement(s)));
  }

  public final void flushBatches() {
    if (!preparedStatements.isEmpty()) {
      log.debug("Flushing batches");
      for (PreparedStatement statement : preparedStatements.values()) {
        uncheck(statement::executeBatch);
      }
    }
  }

  public final void processHooks() {
    if (!postCommitHooks.isEmpty()) {
      log.debug("Running post-commit hooks");
      postCommitHooks.stream().map(Supplier::get).forEach(this::runHook);
    }
  }

  @SneakyThrows
  private void runHook(CompletableFuture<Void> hook) {
    Utils.join(hook);
  }

  void commit() {
    uncheck(connection::commit);
  }

  void rollback() throws SQLException {
    connection.rollback();
  }

  @SuppressWarnings("unchecked")
  @Override
  public CONTEXT context() {
    return context;
  }

  @Override
  public void close() {
    if (!preparedStatements.isEmpty()) {
      log.debug("Closing batch statements");
      safelyClose(preparedStatements.values());
    }
  }
}
