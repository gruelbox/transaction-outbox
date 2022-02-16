package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.jdbc.SimpleTransaction;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

/**
 * A stub transaction manager that assumes no underlying database, and a transaction context of the
 * specified type.
 *
 * @deprecated Use {@link
 *     com.gruelbox.transactionoutbox.jdbc.StubParameterContextJdbcTransactionManager} for
 *     equivalent functionality.
 */
@Slf4j
@Deprecated
public class StubParameterContextTransactionManager<CX>
    implements ParameterContextTransactionManager<CX> {

  private final Class<CX> contextClass;
  private final Supplier<CX> contextFactory;
  private final ConcurrentMap<CX, JdbcShimTransaction> contextMap = new ConcurrentHashMap<>();

  /**
   * @param contextClass The class that represents the context. Must support equals/hashCode.
   * @param contextFactory Generates context instances when transactions are started.
   */
  @Beta
  public StubParameterContextTransactionManager(
      Class<CX> contextClass, Supplier<CX> contextFactory) {
    this.contextClass = contextClass;
    this.contextFactory = contextFactory;
  }

  @Override
  public Transaction transactionFromContext(CX context) {
    return contextMap.get(context);
  }

  @Override
  public final Class<CX> contextType() {
    return contextClass;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T, E extends Exception> T inTransactionReturnsThrows(
      ThrowingTransactionalSupplier<T, E> work) throws E {
    return withTransaction(
        atx -> {
          T result = work.doWork(atx);
          ((SimpleTransaction<Void>) ((JdbcShimTransaction) atx).getDelegate()).processHooks();
          return result;
        });
  }

  private <T, E extends Exception> T withTransaction(ThrowingTransactionalSupplier<T, E> work)
      throws E {
    Connection mockConnection = Utils.createLoggingProxy(new ProxyFactory(), Connection.class);
    CX context = contextFactory.get();
    try (SimpleTransaction<CX> tx = new SimpleTransaction<>(mockConnection, context)) {
      JdbcShimTransaction shim = new JdbcShimTransaction(tx);
      contextMap.put(context, shim);
      try {
        return work.doWork(shim);
      } finally {
        contextMap.remove(context);
      }
    }
  }
}
