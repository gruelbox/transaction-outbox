package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.spi.InitializationEventBus;
import com.gruelbox.transactionoutbox.spi.InitializationEventPublisher;
import com.gruelbox.transactionoutbox.spi.SerializableTypeRequired;
import com.gruelbox.transactionoutbox.spi.ThrowingTransactionalSupplier;
import com.gruelbox.transactionoutbox.spi.TransactionManagerSupport;
import java.lang.reflect.Method;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

@Beta
@Slf4j
@Service
class SpringTransactionManagerImpl
    implements SpringTransactionManager, InitializationEventPublisher {

  private final SpringTransaction transaction;
  private final TransactionTemplate transactionTemplate;

  @Autowired
  SpringTransactionManagerImpl(DataSource dataSource, TransactionTemplate transactionTemplate) {
    this.transaction = new SpringTransactionImpl(dataSource);
    this.transactionTemplate = transactionTemplate;
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
    try {
      return transactionTemplate.execute(
          ts -> {
            try {
              return work.doWork(transaction);
            } catch (Exception e) {
              throw new UncheckedException(e);
            }
          });
    } catch (UncheckedException e) {
      throw Utils.sneakyThrow(e.getCause());
    }
  }

  @Override
  public TransactionalInvocation extractTransaction(Method method, Object[] args) {
    return TransactionManagerSupport.toTransactionalInvocation(method, args, transaction);
  }

  @Override
  public Invocation injectTransaction(Invocation invocation, SpringTransaction transaction) {
    return TransactionManagerSupport.injectTransactionIntoInvocation(
        invocation, Void.class, transaction);
  }

  @Override
  public void onPublishInitializationEvents(InitializationEventBus eventBus) {
    eventBus.sendEvent(
        SerializableTypeRequired.class, new SerializableTypeRequired(SpringTransaction.class));
  }
}
