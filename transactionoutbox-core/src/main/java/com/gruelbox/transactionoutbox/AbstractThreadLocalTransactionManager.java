package com.gruelbox.transactionoutbox;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuperBuilder
@NoArgsConstructor(access = AccessLevel.PROTECTED)
abstract class AbstractThreadLocalTransactionManager<TX extends SimpleTransaction>
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
  public <T, E extends Exception> T requireTransactionReturns(
      ThrowingTransactionalSupplier<T, E> work) throws E, NoTransactionActiveException {
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
