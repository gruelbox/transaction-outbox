package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.spi.InitializationEventBus;
import com.gruelbox.transactionoutbox.spi.InitializationEventPublisher;
import com.gruelbox.transactionoutbox.spi.SerializableTypeRequired;
import com.gruelbox.transactionoutbox.spi.ThrowingTransactionalSupplier;
import com.gruelbox.transactionoutbox.spi.TransactionManagerSupport;
import java.lang.reflect.Method;
import lombok.extern.slf4j.Slf4j;
import org.jooq.Configuration;
import org.jooq.DSLContext;

@Slf4j
@Beta
final class DefaultJooqTransactionManagerImpl
    implements DefaultJooqTransactionManager, InitializationEventPublisher {

  private final DSLContext dsl;

  DefaultJooqTransactionManagerImpl(DSLContext dsl) {
    this.dsl = dsl;
  }

  @Override
  public void onPublishInitializationEvents(InitializationEventBus eventBus) {
    eventBus.sendEvent(
        SerializableTypeRequired.class, new SerializableTypeRequired(JooqTransaction.class));
    eventBus.sendEvent(
        SerializableTypeRequired.class, new SerializableTypeRequired(Configuration.class));
  }

  @Override
  public <T, E extends Exception> T inTransactionReturnsThrows(
      ThrowingTransactionalSupplier<T, E, JooqTransaction> work) {
    return dsl.transactionResult(cfg -> work.doWork(transactionFromContext(cfg)));
  }

  @Override
  public TransactionalInvocation extractTransaction(Method method, Object[] args) {
    return TransactionManagerSupport.extractTransactionFromInvocation(
        method, args, Configuration.class, this::transactionFromContext);
  }

  @Override
  public Invocation injectTransaction(Invocation invocation, JooqTransaction transaction) {
    return TransactionManagerSupport.injectTransactionIntoInvocation(
        invocation, Configuration.class, transaction);
  }

  private JooqTransaction transactionFromContext(Configuration context) {
    Object txn = context.data(JooqTransactionListener.TXN_KEY);
    if (txn == null) {
      throw new IllegalStateException(
          JooqTransactionListener.class.getSimpleName() + " is not attached to the DSL");
    }
    return (JooqTransaction) txn;
  }
}
