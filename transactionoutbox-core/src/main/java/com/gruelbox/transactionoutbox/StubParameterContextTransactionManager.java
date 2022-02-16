package com.gruelbox.transactionoutbox;

import java.sql.Connection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

/**
 * A stub transaction manager that assumes no underlying database, and a transaction context of the
 * specified type.
 */
@Slf4j
public class StubParameterContextTransactionManager<C>
    implements ParameterContextTransactionManager<C> {

  private final Class<C> contextClass;
  private final Supplier<C> contextFactory;
  private final ConcurrentMap<C, Transaction> contextMap = new ConcurrentHashMap<>();

  /**
   * @param contextClass The class that represents the context. Must support equals/hashCode.
   * @param contextFactory Generates context instances when transactions are started.
   */
  @Beta
  public StubParameterContextTransactionManager(Class<C> contextClass, Supplier<C> contextFactory) {
    this.contextClass = contextClass;
    this.contextFactory = contextFactory;
  }

  @Override
  public Transaction transactionFromContext(C context) {
    return contextMap.get(context);
  }

  @Override
  public final Class<C> contextType() {
    return contextClass;
  }

  @Override
  public <T, E extends Exception> T inTransactionReturnsThrows(
      ThrowingTransactionalSupplier<T, E> work) throws E {
    return withTransaction(
        atx -> {
          T result = work.doWork(atx);
          ((SimpleTransaction) atx).processHooks();
          return result;
        });
  }

  private <T, E extends Exception> T withTransaction(ThrowingTransactionalSupplier<T, E> work)
      throws E {
    Connection mockConnection = Utils.createLoggingProxy(new ProxyFactory(), Connection.class);
    C context = contextFactory.get();
    try (SimpleTransaction transaction = new SimpleTransaction(mockConnection, context)) {
      contextMap.put(context, transaction);
      return work.doWork(transaction);
    } finally {
      contextMap.remove(context);
    }
  }
}
