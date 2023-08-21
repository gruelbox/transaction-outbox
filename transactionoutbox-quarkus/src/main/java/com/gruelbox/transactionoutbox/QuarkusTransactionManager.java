package com.gruelbox.transactionoutbox;

import static com.gruelbox.transactionoutbox.Utils.uncheck;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.sql.DataSource;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.TransactionSynchronizationRegistry;
import javax.transaction.Transactional;
import javax.transaction.Transactional.TxType;

/** Transaction manager which uses cdi and quarkus. */
@ApplicationScoped
public class QuarkusTransactionManager implements ThreadLocalContextTransactionManager {

  private final CdiTransaction transactionInstance = new CdiTransaction();

  private final DataSource datasource;

  private final TransactionSynchronizationRegistry tsr;

  @Inject
  public QuarkusTransactionManager(DataSource datasource, TransactionSynchronizationRegistry tsr) {
    this.datasource = datasource;
    this.tsr = tsr;
  }

  @Override
  @Transactional(value = TxType.REQUIRES_NEW)
  public void inTransaction(Runnable runnable) {
    uncheck(() -> inTransactionReturnsThrows(ThrowingTransactionalSupplier.fromRunnable(runnable)));
  }

  @Override
  @Transactional(value = TxType.REQUIRES_NEW)
  public void inTransaction(TransactionalWork work) {
    uncheck(() -> inTransactionReturnsThrows(ThrowingTransactionalSupplier.fromWork(work)));
  }

  @Override
  @Transactional(value = TxType.REQUIRES_NEW)
  public <T, E extends Exception> T inTransactionReturnsThrows(
      ThrowingTransactionalSupplier<T, E> work) throws E {
    return work.doWork(transactionInstance);
  }

  @Override
  public <T, E extends Exception> T requireTransactionReturns(
      ThrowingTransactionalSupplier<T, E> work) throws E, NoTransactionActiveException {
    if (tsr.getTransactionStatus() != Status.STATUS_ACTIVE) {
      throw new NoTransactionActiveException();
    }

    return work.doWork(transactionInstance);
  }

  private final class CdiTransaction implements Transaction {

    public final Connection connection() {
      try {
        return datasource.getConnection();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public PreparedStatement prepareBatchStatement(String sql) {
      BatchCountingStatement preparedStatement =
          Utils.uncheckedly(
              () -> BatchCountingStatementHandler.countBatches(connection().prepareStatement(sql)));

      tsr.registerInterposedSynchronization(
          new Synchronization() {
            @Override
            public void beforeCompletion() {
              if (preparedStatement.getBatchCount() != 0) {
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
      tsr.registerInterposedSynchronization(
          new Synchronization() {
            @Override
            public void beforeCompletion() {}

            @Override
            public void afterCompletion(int status) {
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
