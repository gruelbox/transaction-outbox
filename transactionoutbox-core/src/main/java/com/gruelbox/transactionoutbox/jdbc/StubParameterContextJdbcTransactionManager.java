package com.gruelbox.transactionoutbox.jdbc;

import com.gruelbox.transactionoutbox.Beta;
import com.gruelbox.transactionoutbox.Invocation;
import com.gruelbox.transactionoutbox.TransactionalInvocation;
import com.gruelbox.transactionoutbox.Utils;
import com.gruelbox.transactionoutbox.spi.ThrowingTransactionalSupplier;
import com.gruelbox.transactionoutbox.spi.TransactionManagerSupport;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

/**
 * A stub transaction manager that assumes no underlying database, and a transaction context of the
 * specified type.
 */
@Slf4j
public class StubParameterContextJdbcTransactionManager<CX, TX extends JdbcTransaction>
    implements JdbcTransactionManager<TX> {

  private final Class<CX> contextClass;
  private final Supplier<CX> contextFactory;
  private final Function<CX, TX> transactionFactory;
  private final ConcurrentMap<CX, TX> contextMap = new ConcurrentHashMap<>();

  /**
   * @param contextClass The class that represents the context. Must support equals/hashCode.
   * @param contextFactory Generates context instances when transactions are started.
   */
  @Beta
  public StubParameterContextJdbcTransactionManager(
      Class<CX> contextClass, Supplier<CX> contextFactory, Function<CX, TX> transactionFactory) {
    this.contextClass = contextClass;
    this.contextFactory = contextFactory;
    this.transactionFactory = transactionFactory;
  }

  @Override
  public <T, E extends Exception> T inTransactionReturnsThrows(
      ThrowingTransactionalSupplier<T, E, TX> work) throws E {
    return withTransaction(
        atx -> {
          T result = work.doWork(atx);
          if (atx instanceof SimpleTransaction) {
            ((SimpleTransaction) atx).processHooks();
          }
          return result;
        });
  }

  private <T, E extends Exception> T withTransaction(ThrowingTransactionalSupplier<T, E, TX> work)
      throws E {
    CX context = contextFactory.get();
    TX tx = transactionFactory.apply(context);
    try {
      contextMap.put(context, tx);
      try {
        return work.doWork(tx);
      } finally {
        contextMap.remove(context);
      }
    } finally {
      if (tx instanceof AutoCloseable) {
        Utils.safelyClose((AutoCloseable) tx);
      }
    }
  }

  @Override
  public TransactionalInvocation extractTransaction(Method method, Object[] args) {
    return TransactionManagerSupport.extractTransactionFromInvocation(
        method, args, contextClass, contextMap::get);
  }

  @Override
  public Invocation injectTransaction(Invocation invocation, TX transaction) {
    return TransactionManagerSupport.injectTransactionIntoInvocation(
        invocation, contextClass, transaction);
  }
}
