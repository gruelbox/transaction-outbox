package com.gruelbox.transactionoutbox.jdbc;

import static com.ea.async.Async.await;
import static com.gruelbox.transactionoutbox.Utils.blockingRun;
import static com.gruelbox.transactionoutbox.Utils.toBlockingFuture;
import static java.util.concurrent.CompletableFuture.completedFuture;

import com.gruelbox.transactionoutbox.Beta;
import com.gruelbox.transactionoutbox.ThrowingTransactionalSupplier;
import com.gruelbox.transactionoutbox.Utils;
import com.gruelbox.transactionoutbox.spi.ParameterContextTransactionManager;
import java.sql.Connection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

/**
 * A stub JDBC transaction manager that assumes no underlying database, and a transaction context of
 * the specified type.
 */
@Slf4j
public class StubParameterContextJdbcTransactionManager<CX, TX extends JdbcTransaction<CX>>
    implements ParameterContextTransactionManager<Connection, CX, TX>,
        JdbcTransactionManager<CX, TX> {

  private final Class<CX> contextClass;
  private final Supplier<CX> contextFactory;
  private final Function<CX, TX> transactionFactory;
  private final ConcurrentMap<CX, TX> contextMap = new ConcurrentHashMap<>();

  @Beta
  public StubParameterContextJdbcTransactionManager(
      Class<CX> contextClass, Supplier<CX> contextFactory, Function<CX, TX> transactionFactory) {
    this.contextClass = contextClass;
    this.contextFactory = contextFactory;
    this.transactionFactory = transactionFactory;
  }

  @Override
  public TX transactionFromContext(CX context) {
    return contextMap.get(context);
  }

  @Override
  public final Class<CX> contextType() {
    return contextClass;
  }

  @Override
  public <T> CompletableFuture<T> transactionally(Function<TX, CompletableFuture<T>> work) {
    CX context = contextFactory.get();
    TX transaction = transactionFactory.apply(context);
    try {
      contextMap.put(context, transaction);
      T result = await(work.apply(transaction));
      if (transaction instanceof SimpleTransaction) {
        ((SimpleTransaction) transaction).processHooks();
      }
      return completedFuture(result);
    } catch (CompletionException e) {
      throw (RuntimeException) Utils.uncheckAndThrow(e.getCause());
    } finally {
      contextMap.remove(context);
    }
  }

  @Override
  public <T, E extends Exception> T inTransactionReturnsThrows(
      ThrowingTransactionalSupplier<T, E, TX> work) {
    return blockingRun(transactionally(tx -> toBlockingFuture(() -> work.doWork(tx))));
  }
}
