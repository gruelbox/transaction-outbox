package com.gruelbox.transactionoutbox.quarkus;

import static com.gruelbox.transactionoutbox.spi.Utils.uncheck;

import com.gruelbox.transactionoutbox.*;
import com.gruelbox.transactionoutbox.spi.BatchCountingStatement;
import com.gruelbox.transactionoutbox.spi.Utils;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;
import jakarta.transaction.TransactionSynchronizationRegistry;
import jakarta.transaction.Transactional;
import jakarta.transaction.Transactional.TxType;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.sql.DataSource;

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

    public Connection connection() {
      try {
        return datasource.getConnection();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
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
      tsr.putResource(QuarkusTransactionManager.this, created);
      tsr.registerInterposedSynchronization(created);
      return created;
    }

    /**
     * The statements are held as a resource of the JTA transaction, which is discarded when the
     * transaction completes.
     *
     * @return The current transaction's statements, or null if none have been prepared.
     */
    private BatchStatements currentBatchStatements() {
      return (BatchStatements) tsr.getResource(QuarkusTransactionManager.this);
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

  /**
   * The batch statements of one transaction, one per SQL string, whose batches are sent just before
   * the transaction completes and which are closed when it has.
   */
  private final class BatchStatements implements Synchronization {

    private final Map<String, BatchCountingStatement> statements = new LinkedHashMap<>();

    PreparedStatement prepare(String sql) {
      return statements.computeIfAbsent(
          sql,
          key ->
              Utils.uncheckedly(
                  () ->
                      BatchCountingStatement.countBatches(
                          transactionInstance.connection().prepareStatement(key))));
    }

    void sendBatches() {
      for (BatchCountingStatement statement : statements.values()) {
        if (statement.getBatchCount() != 0) {
          Utils.uncheck(statement::executeBatch);
        }
      }
    }

    @Override
    public void beforeCompletion() {
      sendBatches();
    }

    @Override
    public void afterCompletion(int status) {
      Utils.safelyClose(statements.values());
    }
  }
}
