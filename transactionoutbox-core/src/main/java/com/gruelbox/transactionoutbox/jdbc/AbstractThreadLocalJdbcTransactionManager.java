package com.gruelbox.transactionoutbox.jdbc;

import com.gruelbox.transactionoutbox.NoTransactionActiveException;
import com.gruelbox.transactionoutbox.ThreadLocalContextTransactionManager;
import com.gruelbox.transactionoutbox.ThrowingTransactionalSupplier;
import java.sql.Connection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PROTECTED)
abstract class AbstractThreadLocalJdbcTransactionManager<CX, TX extends SimpleTransaction<CX>>
    implements ThreadLocalContextTransactionManager<Connection, CX, TX>,
        JdbcTransactionManager<CX, TX> {

  private final ThreadLocal<Deque<TX>> transactions = ThreadLocal.withInitial(LinkedList::new);

  @Override
  public final void inTransaction(Runnable runnable) {
    JdbcTransactionManager.super.inTransaction(runnable);
  }

  @Override
  public final void inTransaction(TransactionalWork<TX> work) {
    JdbcTransactionManager.super.inTransaction(work);
  }

  @Override
  public final <T> T inTransactionReturns(TransactionalSupplier<T, TX> supplier) {
    return JdbcTransactionManager.super.inTransactionReturns(supplier);
  }

  @Override
  public final <E extends Exception> void inTransactionThrows(ThrowingTransactionalWork<E, TX> work)
      throws E {
    JdbcTransactionManager.super.inTransactionThrows(work);
  }

  @Override
  public <T, E extends Exception> T requireTransactionReturns(
      ThrowingTransactionalSupplier<T, E, TX> work) throws E, NoTransactionActiveException {
    return work.doWork(peekTransaction().orElseThrow(NoTransactionActiveException::new));
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
}
