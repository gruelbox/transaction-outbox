package com.gruelbox.transactionoutbox;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.persistence.EntityManager;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.internal.SessionImpl;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/** Transaction manager which uses spring-tx and Hibernate. */
@Slf4j
@SuperBuilder
public class SpringTransactionManager implements TransactionManager {

  private final ThreadLocal<ArrayList<Runnable>> postCommitHooks = new ThreadLocal<>();
  private final ThreadLocal<Map<String, PreparedStatement>> statements = new ThreadLocal<>();

  /** The Hibernate entity manager. */
  @NonNull private final EntityManager entityManager;

  @Override
  public Connection getActiveConnection() {
    return ((SessionImpl) entityManager.getDelegate()).connection(); /* TODO rank */
  }

  @Override
  public <T> T inTransactionReturnsThrows(Callable<T> callable) throws Exception {
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

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  private <T> T transaction(Callable<T> callable) throws Exception {
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
    }
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
