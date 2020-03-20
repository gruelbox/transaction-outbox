package com.gruelbox.transactionoutbox;

import static com.gruelbox.transactionoutbox.Utils.safelyClose;
import static com.gruelbox.transactionoutbox.Utils.uncheck;

import com.gruelbox.transactionoutbox.AbstractThreadLocalTransactionManager.ThreadLocalTransaction;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuperBuilder
@NoArgsConstructor(access = AccessLevel.PROTECTED)
abstract class AbstractThreadLocalTransactionManager<TX extends ThreadLocalTransaction>
    implements TransactionManager {

  private final ThreadLocal<Deque<TX>> transactions = ThreadLocal.withInitial(LinkedList::new);

  @Override
  public final void inTransaction(Runnable runnable) {
    TransactionManager.super.inTransaction(runnable);
  }

  @Override
  public final void inTransaction(TransactionalWork work) {
    TransactionManager.super.inTransaction(work);
  }

  @Override
  public final <T> T inTransactionReturns(TransactionalSupplier<T> supplier) {
    return TransactionManager.super.inTransactionReturns(supplier);
  }

  @Override
  public final <E extends Exception> void inTransactionThrows(ThrowingTransactionalWork<E> work)
      throws E {
    TransactionManager.super.inTransactionThrows(work);
  }

  @Override
  public final <E extends Exception> void requireTransaction(ThrowingTransactionalWork<E> work)
      throws E {
    work.doWork(peekTransaction().orElseThrow(NoTransactionActiveException::new));
  }

  final TX pushTransaction(TX transaction) {
    transactions.get().push(transaction);
    return transaction;
  }

  final TX popTransaction() {
    TX result = transactions.get().pop();
    if (transactions.get().isEmpty()) {
      transactions.remove();
    }
    return result;
  }

  Optional<TX> peekTransaction() {
    return Optional.ofNullable(transactions.get().peek());
  }

  @AllArgsConstructor(access = AccessLevel.PROTECTED)
  protected static class ThreadLocalTransaction implements Transaction, AutoCloseable {

    private final List<Runnable> postCommitHooks = new ArrayList<>();
    private final Map<String, PreparedStatement> preparedStatements = new HashMap<>();
    private final Connection connection;

    @Override
    public final Connection connection() {
      return connection;
    }

    @Override
    public final void addPostCommitHook(Runnable runnable) {
      postCommitHooks.add(runnable);
    }

    @Override
    public final PreparedStatement prepareBatchStatement(String sql) {
      return preparedStatements.computeIfAbsent(
          sql, s -> Utils.uncheckedly(() -> connection.prepareStatement(s)));
    }

    final void flushBatches() {
      if (!preparedStatements.isEmpty()) {
        log.debug("Flushing batches");
        for (PreparedStatement statement : preparedStatements.values()) {
          uncheck(statement::executeBatch);
        }
      }
    }

    final void processHooks() {
      if (!postCommitHooks.isEmpty()) {
        log.debug("Running post-commit hooks");
        postCommitHooks.forEach(Runnable::run);
      }
    }

    void commit() {
      uncheck(connection::commit);
    }

    void rollback() throws SQLException {
      connection.rollback();
    }

    @Override
    public void close() {
      if (!preparedStatements.isEmpty()) {
        log.debug("Closing batch statements");
        safelyClose(preparedStatements.values());
      }
    }
  }
}
