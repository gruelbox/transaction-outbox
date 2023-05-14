package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.jdbc.JdbcTransactionManager;
import com.gruelbox.transactionoutbox.spi.*;
import com.gruelbox.transactionoutbox.spi.ThrowingTransactionalSupplier;
import java.lang.reflect.Method;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.sql.DataSource;
import javax.transaction.TransactionSynchronizationRegistry;
import javax.transaction.Transactional;
import javax.transaction.Transactional.TxType;

/** Transaction manager which uses cdi and quarkus. */
@ApplicationScoped
public class QuarkusTransactionManager
    implements JdbcTransactionManager<CdiTransaction>, InitializationEventPublisher {

  private final CdiTransaction transactionInstance;

  @Inject
  public QuarkusTransactionManager(DataSource datasource, TransactionSynchronizationRegistry tsr) {
    this.transactionInstance = new CdiTransaction(datasource, tsr);
  }

  @Override
  @Transactional(value = TxType.REQUIRES_NEW)
  public <T, E extends Exception> T inTransactionReturnsThrows(
      ThrowingTransactionalSupplier<T, E, CdiTransaction> work) throws E {
    return work.doWork(transactionInstance);
  }

  @Override
  public TransactionalInvocation extractTransaction(Method method, Object[] args) {
    return TransactionManagerSupport.toTransactionalInvocation(method, args, transactionInstance);
  }

  @Override
  public Invocation injectTransaction(Invocation invocation, CdiTransaction transaction) {
    return TransactionManagerSupport.injectTransactionIntoInvocation(
        invocation, Void.class, transaction);
  }

  @Override
  public void onPublishInitializationEvents(InitializationEventBus eventBus) {
    eventBus.sendEvent(
        SerializableTypeRequired.class, new SerializableTypeRequired(CdiTransaction.class));
  }
}
