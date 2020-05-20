package com.gruelbox.transactionoutbox;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.internal.SessionImpl;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/** Transaction manager which uses spring-tx and Hibernate. */
@Slf4j
@Service
public class SpringTransactionManager implements ThreadLocalContextTransactionManager {

  private final SpringTransaction transactionInstance = new SpringTransaction();

  @PersistenceContext private EntityManager entityManager;

  @Override
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public <T, E extends Exception> T inTransactionReturnsThrows(
      ThrowingTransactionalSupplier<T, E> work) throws E {
    return work.doWork(transactionInstance);
  }

  @Override
  public <T, E extends Exception> T requireTransactionReturns(
      ThrowingTransactionalSupplier<T, E> work) throws E, NoTransactionActiveException {
    return work.doWork(transactionInstance);
  }

  private final class SpringTransaction implements Transaction {

    @Override
    public Connection connection() {
      try {
        SessionImpl delegate = (SessionImpl) entityManager.getDelegate();
        return delegate.connection();
      } catch (RuntimeException e) {
        throw new NoTransactionActiveException(e);
      }
    }

    @Override
    public PreparedStatement prepareBatchStatement(String sql) {
      BatchCountingStatement preparedStatement =
          Utils.uncheckedly(
              () -> BatchCountingStatementHandler.countBatches(connection().prepareStatement(sql)));
      TransactionSynchronizationManager.registerSynchronization(
          new TransactionSynchronizationAdapter() {
            @Override
            public void beforeCommit(boolean readOnly) {
              if (preparedStatement.getBatchCount() != 0) {
                log.debug("Flushing batches");
                Utils.uncheck(preparedStatement::executeBatch);
              }
            }

            @Override
            public void afterCompletion(int status) {
              Utils.safelyClose(preparedStatement);
            }
          });
      return preparedStatement;
    }

    @Override
    public void addPostCommitHook(Runnable runnable) {
      TransactionSynchronizationManager.registerSynchronization(
          new TransactionSynchronizationAdapter() {
            @Override
            public void afterCommit() {
              runnable.run();
            }
          });
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
      } finally {
        if ("addBatch".equals(method.getName())) {
          ++count;
        }
      }
    }
  }
}
