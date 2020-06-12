package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.spi.ThrowingTransactionalSupplier;
import com.gruelbox.transactionoutbox.spi.TransactionManagerSupport;
import java.lang.reflect.Method;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Beta
@Slf4j
@Service
class SpringTransactionManagerImpl implements SpringTransactionManager {

  private final SpringTransaction transaction;
  private final SpringTransactionEntryPoints entryPoints;

  @Autowired
  SpringTransactionManagerImpl(DataSource dataSource, SpringTransactionEntryPoints entryPoints) {
    this.transaction = new SpringTransactionImpl(dataSource);
    this.entryPoints = entryPoints;
  }

  @Override
  public <T, E extends Exception> T requireTransactionReturns(
      ThrowingTransactionalSupplier<T, E, SpringTransaction> work)
      throws E, NoTransactionActiveException {
    if (!TransactionSynchronizationManager.isActualTransactionActive()) {
      throw new NoTransactionActiveException();
    }
    return work.doWork(transaction);
  }

  @Override
  public <T, E extends Exception> T inTransactionReturnsThrows(
      ThrowingTransactionalSupplier<T, E, SpringTransaction> work) throws E {
    return entryPoints.inTransactionReturnsThrows(work, transaction);
  }

  @Override
  public TransactionalInvocation extractTransaction(Method method, Object[] args) {
    return TransactionManagerSupport.toTransactionalInvocation(method, args, transaction);
  }

  @Override
  public Invocation injectTransaction(Invocation invocation, SpringTransaction transaction) {
    return invocation;
  }
}
