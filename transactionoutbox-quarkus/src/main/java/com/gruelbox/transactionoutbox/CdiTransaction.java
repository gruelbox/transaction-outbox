package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.jdbc.JdbcTransaction;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import javax.sql.DataSource;
import javax.transaction.Synchronization;
import javax.transaction.TransactionSynchronizationRegistry;

public final class CdiTransaction implements JdbcTransaction {

  private final DataSource datasource;
  private final TransactionSynchronizationRegistry tsr;

  CdiTransaction(DataSource dataSource, TransactionSynchronizationRegistry tsr) {
    this.datasource = dataSource;
    this.tsr = tsr;
  }

  @Override
  public Connection connection() {
    try {
      return datasource.getConnection();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public PreparedStatement prepareBatchStatement(String sql) {
    @SuppressWarnings("resource")
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

  @Override
  public void addPostCommitHook(Supplier<CompletableFuture<Void>> hook) {
    tsr.registerInterposedSynchronization(
        new Synchronization() {
          @Override
          public void beforeCompletion() {}

          @Override
          public void afterCompletion(int status) {
            Utils.join(hook.get());
          }
        });
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
