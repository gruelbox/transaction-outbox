package com.gruelbox.transactionoutbox;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import javax.sql.DataSource;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@AllArgsConstructor(access = AccessLevel.PACKAGE)
@Slf4j
final class SpringTransactionImpl implements SpringTransaction {

  private DataSource dataSource;

  @Override
  public Connection connection() {
    return DataSourceUtils.getConnection(dataSource);
  }

  @Override
  public <T> T context() {
    return null;
  }

  @Override
  public void addPostCommitHook(Supplier<CompletableFuture<Void>> hook) {
    TransactionSynchronizationManager.registerSynchronization(
        new TransactionSynchronization() {
          @Override
          @SneakyThrows
          public void afterCommit() {
            Utils.join(hook.get());
          }
        });
  }

  @Override
  public PreparedStatement prepareBatchStatement(String sql) {
    BatchCountingStatement preparedStatement =
        Utils.uncheckedly(
            () -> BatchCountingStatementHandler.countBatches(connection().prepareStatement(sql)));
    TransactionSynchronizationManager.registerSynchronization(
        new TransactionSynchronization() {
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
