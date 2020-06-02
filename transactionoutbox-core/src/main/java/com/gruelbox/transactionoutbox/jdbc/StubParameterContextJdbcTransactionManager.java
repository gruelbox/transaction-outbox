package com.gruelbox.transactionoutbox.jdbc;

import static com.ea.async.Async.await;
import static com.gruelbox.transactionoutbox.Utils.blockingRun;
import static com.gruelbox.transactionoutbox.Utils.toBlockingFuture;
import static java.util.concurrent.CompletableFuture.completedFuture;

import com.gruelbox.transactionoutbox.Beta;
import com.gruelbox.transactionoutbox.ParameterContextTransactionManager;
import com.gruelbox.transactionoutbox.ThrowingTransactionalSupplier;
import com.gruelbox.transactionoutbox.Utils;
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
public class StubParameterContextJdbcTransactionManager<C>
    implements ParameterContextTransactionManager<Connection, C, JdbcTransaction<C>>,
        JdbcTransactionManager<C, JdbcTransaction<C>> {

  private final Class<C> contextClass;
  private final Supplier<C> contextFactory;
  private final ConcurrentMap<C, JdbcTransaction<C>> contextMap = new ConcurrentHashMap<>();

  /**
   * @param contextClass The class that represents the context. Must support equals/hashCode.
   * @param contextFactory Generates context instances when transactions are started.
   */
  @Beta
  public StubParameterContextJdbcTransactionManager(
      Class<C> contextClass, Supplier<C> contextFactory) {
    this.contextClass = contextClass;
    this.contextFactory = contextFactory;
  }

  @Override
  public JdbcTransaction<C> transactionFromContext(C context) {
    return contextMap.get(context);
  }

  @Override
  public final Class<C> contextType() {
    return contextClass;
  }

  @Override
  public <T> CompletableFuture<T> transactionally(
      Function<JdbcTransaction<C>, CompletableFuture<T>> work) {
    Connection mockConnection = Utils.createLoggingProxy(Connection.class);
    C context = contextFactory.get();
    try (SimpleTransaction<C> transaction = new SimpleTransaction<>(mockConnection, context)) {
      contextMap.put(context, transaction);
      T result = await(work.apply(transaction));
      transaction.processHooks();
      return completedFuture(result);
    } catch (CompletionException e) {
      throw (RuntimeException) Utils.uncheckAndThrow(e.getCause());
    } finally {
      contextMap.remove(context);
    }
  }

  @Override
  public <T, E extends Exception> T inTransactionReturnsThrows(
      ThrowingTransactionalSupplier<T, E, JdbcTransaction<C>> work) {
    return blockingRun(transactionally(tx -> toBlockingFuture(() -> work.doWork(tx))));
  }
}
