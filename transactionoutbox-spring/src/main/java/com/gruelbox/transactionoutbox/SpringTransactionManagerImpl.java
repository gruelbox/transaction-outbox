package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.spi.ThrowingTransactionalSupplier;
import com.gruelbox.transactionoutbox.spi.TransactionManagerSupport;
import java.lang.reflect.Method;
import javax.persistence.EntityManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Beta
@Slf4j
@Service
public class SpringTransactionManagerImpl implements SpringTransactionManager {

  private final SpringTransaction transaction;
  private final SpringTransactionEntryPoints entryPoints;

  public SpringTransactionManagerImpl(
      EntityManager entityManager, SpringTransactionEntryPoints entryPoints) {
    this.transaction = new SpringTransaction(entityManager);
    this.entryPoints = entryPoints;
  }

  @Override
  public <T, E extends Exception> T requireTransactionReturns(
      ThrowingTransactionalSupplier<T, E, SpringTransaction> work)
      throws E, NoTransactionActiveException {
    return entryPoints.requireTransactionReturns(work, transaction);
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
