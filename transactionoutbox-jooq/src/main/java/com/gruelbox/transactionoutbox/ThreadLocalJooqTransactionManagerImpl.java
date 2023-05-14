package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.jdbc.AbstractThreadLocalJdbcTransactionManager;
import com.gruelbox.transactionoutbox.spi.InitializationEventBus;
import com.gruelbox.transactionoutbox.spi.InitializationEventPublisher;
import com.gruelbox.transactionoutbox.spi.SerializableTypeRequired;
import com.gruelbox.transactionoutbox.spi.ThrowingTransactionalSupplier;
import com.gruelbox.transactionoutbox.spi.TransactionManagerSupport;
import java.lang.reflect.Method;
import lombok.extern.slf4j.Slf4j;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;

@Slf4j
final class ThreadLocalJooqTransactionManagerImpl
    extends AbstractThreadLocalJdbcTransactionManager<JooqTransaction>
    implements ThreadLocalJooqTransactionManager, InitializationEventPublisher {

  private final DSLContext parentDsl;

  ThreadLocalJooqTransactionManagerImpl(DSLContext parentDsl) {
    this.parentDsl = parentDsl;
  }

  @Override
  public void onPublishInitializationEvents(InitializationEventBus eventBus) {
    eventBus.sendEvent(
        SerializableTypeRequired.class, new SerializableTypeRequired(JooqTransaction.class));
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T, E extends Exception> T inTransactionReturnsThrows(
      ThrowingTransactionalSupplier<T, E, JooqTransaction> work) throws E {
    DSLContext dsl =
        peekTransaction()
            .map(JooqTransaction::context)
            .map(Configuration.class::cast)
            .map(Configuration::dsl)
            .orElse(parentDsl);
    try {
      return dsl.transactionResult(
          config ->
              config
                  .dsl()
                  .connectionResult(connection -> work.doWork(peekTransaction().orElseThrow())));
    } catch (DataAccessException dax) {
      try {
        throw dax.getCause();
      } catch (Exception e) {
        throw (E) e;
      } catch (Throwable t) {
        throw dax;
      }
    }
  }

  @Override
  public TransactionalInvocation extractTransaction(Method method, Object[] args) {
    return TransactionManagerSupport.extractTransactionFromInvocationOptional(
            method, args, Configuration.class, this::transactionFromContext)
        .orElse(super.extractTransaction(method, args));
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
