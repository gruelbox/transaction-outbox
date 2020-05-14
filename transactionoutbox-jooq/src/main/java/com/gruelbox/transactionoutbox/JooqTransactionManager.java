package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.AbstractThreadLocalTransactionManager.ThreadLocalTransaction;
import java.util.Deque;
import java.util.LinkedList;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.impl.DefaultConfiguration;

/**
 * Transaction manager which uses jOOQ's transaction management. In order to wire into JOOQ's
 * transaction lifecycle, a slightly convoluted construction process is required which involves
 * first creating a {@link JooqTransactionListener}, including it in the JOOQ {@link
 * org.jooq.Configuration} while constructing the root {@link DSLContext}, and then finally linking
 * the listener to the new {@link JooqTransactionManager}:
 *
 * <pre>
 * DataSourceConnectionProvider connectionProvider = new DataSourceConnectionProvider(dataSource);
 * DefaultConfiguration configuration = new DefaultConfiguration();
 * configuration.setConnectionProvider(connectionProvider);
 * configuration.setSQLDialect(SQLDialect.H2);
 * configuration.setTransactionProvider(new ThreadLocalTransactionProvider(connectionProvider));
 * JooqTransactionListener listener = JooqTransactionManager.createListener();
 * configuration.set(listener);
 * DSLContext dsl = DSL.using(configuration);
 * return JooqTransactionManager.create(dsl, listener);</pre>
 */
@Slf4j
public final class JooqTransactionManager
    extends AbstractThreadLocalTransactionManager<ThreadLocalTransaction> {

  private final ThreadLocal<Deque<DSLContext>> currentDsl =
      ThreadLocal.withInitial(LinkedList::new);

  /** The parent jOOQ DSL context. */
  private final DSLContext parentDsl;

  private JooqTransactionManager(DSLContext parentDsl) {
    this.parentDsl = parentDsl;
  }

  /**
   * Creates the {@link org.jooq.TransactionListener} to wire into the {@link DSLContext}. See
   * class-level documentation for more detail.
   *
   * @return The transaction listener.
   */
  public static JooqTransactionListener createListener() {
    return new JooqTransactionListener();
  }

  /**
   * Creates the transaction manager.
   *
   * @param dslContext The DSL context.
   * @param listener The listener, linked to the DSL context.
   * @return The transaction manager.
   */
  public static JooqTransactionManager create(
      DSLContext dslContext, JooqTransactionListener listener) {
    var result = new JooqTransactionManager(dslContext);
    listener.setJooqTransactionManager(result);
    return result;
  }

  @Override
  public <T, E extends Exception> T inTransactionReturnsThrows(
      ThrowingTransactionalSupplier<T, E> work) {
    DSLContext dsl = currentDsl.get().isEmpty() ? parentDsl : currentDsl.get().peek();
    return dsl.transactionResult(
        config ->
            config
                .dsl()
                .connectionResult(connection -> work.doWork(peekTransaction().orElseThrow())));
  }

  void pushContext(DSLContext dsl) {
    currentDsl.get().push(dsl);
  }

  void popContext() {
    currentDsl.get().pop();
    if (currentDsl.get().isEmpty()) {
      currentDsl.remove();
    }
  }
}
