package com.gruelbox.transactionoutbox;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.Callable;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;

/** Transaction manager which uses jOOQ's transaction management. */
@Slf4j
@SuperBuilder
public class JooqTransactionManager implements TransactionManager {

  private final ThreadLocal<ArrayList<Runnable>> postCommitHooks = new ThreadLocal<>();
  private final ThreadLocal<Map<String, PreparedStatement>> statements = new ThreadLocal<>();
  private final ThreadLocal<Deque<DSLContext>> currentDsl = new ThreadLocal<>();
  private final ThreadLocal<Deque<Connection>> currentConnection = new ThreadLocal<>();

  /** The parent jOOQ DSL context. */
  @NonNull private final DSLContext parentDsl;

  @Override
  public Connection getActiveConnection() {
    return currentConnection.get().peek();
  }

  @Override
  public <T> T inTransactionReturnsThrows(Callable<T> callable) {
    try {
      var result = transaction(callable);
      if (postCommitHooks.get() != null) {
        log.debug("Running post-commit hooks");
        postCommitHooks.get().forEach(Runnable::run);
      }
      return result;
    } finally {
      postCommitHooks.remove();
    }
  }

  private <T> T transaction(Callable<T> callable) {
    DSLContext dsl = currentDsl.get() == null ? parentDsl : currentDsl.get().peek();
    return dsl.transactionResult(
        config ->
            config
                .dsl()
                .connectionResult(
                    connection -> {
                      if (currentDsl.get() == null) {
                        currentDsl.set(new LinkedList<>());
                        currentConnection.set(new LinkedList<>());
                      }
                      currentDsl.get().push(dsl);
                      currentConnection.get().push(connection);
                      try {
                        T result = callable.call();
                        if (statements.get() != null) {
                          log.debug("Flushing batches");
                          statements.get().values().forEach(it -> Utils.uncheck(it::executeBatch));
                        }
                        return result;
                      } finally {
                        if (statements.get() != null) {
                          log.debug("Closing batch statements");
                          Utils.safelyClose(statements.get().values());
                          statements.remove();
                        }
                        currentDsl.get().pop();
                        currentConnection.get().pop();
                        if (currentDsl.get().isEmpty()) {
                          currentDsl.remove();
                          currentConnection.remove();
                        }
                      }
                    }));
  }

  @Override
  public void addPostCommitHook(Runnable runnable) {
    if (postCommitHooks.get() == null) postCommitHooks.set(new ArrayList<>());
    postCommitHooks.get().add(runnable);
  }

  @Override
  public PreparedStatement prepareBatchStatement(String sql) {
    if (statements.get() == null) statements.set(new HashMap<>());
    return statements
        .get()
        .computeIfAbsent(
            sql, s -> Utils.uncheckedly(() -> getActiveConnection().prepareStatement(s)));
  }
}
