package com.gruelbox.transactionoutbox.jdbc;

import com.gruelbox.transactionoutbox.Beta;
import com.gruelbox.transactionoutbox.NoTransactionActiveException;
import com.gruelbox.transactionoutbox.NotApi;
import com.gruelbox.transactionoutbox.ThrowingTransactionalSupplier;
import com.gruelbox.transactionoutbox.ThrowingTransactionalWork;
import com.gruelbox.transactionoutbox.TransactionalWork;
import com.gruelbox.transactionoutbox.spi.ThreadLocalContextTransactionManager;
import java.sql.Connection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Beta
@NotApi
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class AbstractThreadLocalJdbcTransactionManager<CX, TX extends JdbcTransaction<CX>>
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

  public final TX pushTransaction(TX transaction) {
    transactions.get().push(transaction);
    return transaction;
  }

  public final TX popTransaction() {
    TX result = transactions.get().pop();
    if (transactions.get().isEmpty()) {
      transactions.remove();
    }
    return result;
  }

  protected Optional<TX> peekTransaction() {
    return Optional.ofNullable(transactions.get().peek());
  }
}
