package com.gruelbox.transactionoutbox.spring;

import static com.gruelbox.transactionoutbox.spi.Utils.uncheck;
import static com.gruelbox.transactionoutbox.spi.Utils.uncheckedly;

import com.gruelbox.transactionoutbox.*;
import com.gruelbox.transactionoutbox.spi.Utils;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Transaction manager which uses spring-tx and Hibernate. */
@Slf4j
@Service
public class SpringTransactionManager implements ThreadLocalContextTransactionManager {

  private final SpringTransaction transactionInstance = new SpringTransaction();
  private final PlatformTransactionManager platformTransactionManager;
  private final DataSource dataSource;

  @Autowired
  public SpringTransactionManager(
      PlatformTransactionManager platformTransactionManager, DataSource dataSource) {
    this.platformTransactionManager = platformTransactionManager;
    this.dataSource = dataSource;
  }

  @Override
  public void inTransaction(Runnable runnable) {
    uncheck(() -> inTransactionReturnsThrows(ThrowingTransactionalSupplier.fromRunnable(runnable)));
  }

  @Override
  public void inTransaction(TransactionalWork work) {
    uncheck(() -> inTransactionReturnsThrows(ThrowingTransactionalSupplier.fromWork(work)));
  }

  @Override
  public <T> T inTransactionReturns(TransactionalSupplier<T> supplier) {
    return uncheckedly(
        () -> inTransactionReturnsThrows(ThrowingTransactionalSupplier.fromSupplier(supplier)));
  }

  @Override
  public <E extends Exception> void inTransactionThrows(ThrowingTransactionalWork<E> work)
      throws E {
    inTransactionReturnsThrows(ThrowingTransactionalSupplier.fromWork(work));
  }

  @Override
  public <T, E extends Exception> T inTransactionReturnsThrows(
      ThrowingTransactionalSupplier<T, E> work) throws E {
    TransactionTemplate transactionTemplate = new TransactionTemplate(platformTransactionManager);
    transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    try {
      return transactionTemplate.execute(
          status -> {
            try {
              return work.doWork(transactionInstance);
            } catch (Exception e) {
              throw new UncheckedException(e);
            }
          });
    } catch (UncheckedException e) {
      @SuppressWarnings("unchecked")
      E cause = (E) e.getCause();
      throw cause;
    }
  }

  @Override
  public <T, E extends Exception> T requireTransactionReturns(
      ThrowingTransactionalSupplier<T, E> work) throws E, NoTransactionActiveException {

    if (!TransactionSynchronizationManager.isActualTransactionActive()) {
      throw new NoTransactionActiveException();
    }

    return work.doWork(transactionInstance);
  }

  private final class SpringTransaction implements Transaction {

    @Override
    public Connection connection() {
      return DataSourceUtils.getConnection(dataSource);
    }

    @Override
    public PreparedStatement prepareBatchStatement(String sql) {
      return batchStatements().prepare(sql);
    }

    @Override
    public void flushBatches() {
      var current = currentBatchStatements();
      if (current != null) {
        current.sendBatches();
      }
    }

    private BatchStatements batchStatements() {
      var current = currentBatchStatements();
      if (current != null) {
        return current;
      }
      var created = new BatchStatements();
      TransactionSynchronizationManager.registerSynchronization(created);
      return created;
    }

    /**
     * The statements are cached on the synchronization that flushes them, not in a resource bound
     * to the thread. Spring only suspends its own resources around a {@code REQUIRES_NEW}
     * transaction, so a cache that outlived the transaction's connection would send an inner
     * transaction's rows down the outer one's.
     *
     * @return The current transaction's statements, or null if none have been prepared.
     */
    private BatchStatements currentBatchStatements() {
      for (TransactionSynchronization synchronization :
          TransactionSynchronizationManager.getSynchronizations()) {
        if (synchronization instanceof BatchStatements batchStatements
            && batchStatements.isOwnedBy(SpringTransactionManager.this)) {
          return batchStatements;
        }
      }
      return null;
    }

    @Override
    public void addPostCommitHook(Runnable runnable) {
      TransactionSynchronizationManager.registerSynchronization(
          new TransactionSynchronization() {
            @Override
            public void afterCommit() {
              runnable.run();
            }
          });
    }
  }

  /**
   * The batch statements of one Spring transaction, one per SQL string, whose batches are sent just
   * before the commit and which are closed when the transaction completes.
   */
  private final class BatchStatements implements TransactionSynchronization {

    private final Map<String, BatchCountingStatement> statements = new LinkedHashMap<>();

    boolean isOwnedBy(SpringTransactionManager manager) {
      return SpringTransactionManager.this == manager;
    }

    PreparedStatement prepare(String sql) {
      return statements.computeIfAbsent(
          sql,
          key ->
              Utils.uncheckedly(
                  () ->
                      BatchCountingStatementHandler.countBatches(
                          DataSourceUtils.getConnection(dataSource).prepareStatement(key))));
    }

    void sendBatches() {
      for (BatchCountingStatement statement : statements.values()) {
        if (statement.getBatchCount() != 0) {
          log.debug("Flushing batches");
          Utils.uncheck(statement::executeBatch);
        }
      }
    }

    @Override
    public void beforeCommit(boolean readOnly) {
      sendBatches();
    }

    @Override
    public void afterCompletion(int status) {
      Utils.safelyClose(statements.values());
    }
  }

  private interface BatchCountingStatement extends PreparedStatement {
    int getBatchCount();
  }

  private static final class BatchCountingStatementHandler implements InvocationHandler {

    private final PreparedStatement delegate;
    private int count = 0;

    private BatchCountingStatementHandler(PreparedStatement delegate) {
      this.delegate = delegate;
    }

    static BatchCountingStatement countBatches(PreparedStatement delegate) {
      return (BatchCountingStatement)
          Proxy.newProxyInstance(
              BatchCountingStatementHandler.class.getClassLoader(),
              new Class[] {BatchCountingStatement.class},
              new BatchCountingStatementHandler(delegate));
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      if ("getBatchCount".equals(method.getName())) {
        return count;
      }
      try {
        return method.invoke(delegate, args);
      } catch (InvocationTargetException e) {
        throw e.getCause();
      } finally {
        if ("addBatch".equals(method.getName())) {
          ++count;
        } else if ("executeBatch".equals(method.getName())) {
          count = 0;
        }
      }
    }
  }
}
