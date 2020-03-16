package com.gruelbox.transactionoutbox;

import java.sql.Connection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.impl.DefaultConfiguration;

/**
 * Transaction manager which uses jOOQ's transaction management. In order to wire into
 * JOOQ's transaction lifecycle, a slightly convoluted construction process is required which
 * involves first creating a {@link JooqTransactionListener}, including it in the JOOQ
 * {@link org.jooq.Configuration} while constructing the root {@link DSLContext}, and then finally
 * linking the listener to the new {@link JooqTransactionManager}:
 *
 * <pre>DataSourceConnectionProvider connectionProvider = new DataSourceConnectionProvider(dataSource);
DefaultConfiguration configuration = new DefaultConfiguration();
configuration.setConnectionProvider(connectionProvider);
configuration.setSQLDialect(SQLDialect.H2);
configuration.setTransactionProvider(new ThreadLocalTransactionProvider(connectionProvider));
JooqTransactionListener listener = JooqTransactionManager.createListener(configuration);
DSLContext dsl = DSL.using(configuration);
return JooqTransactionManager.create(dsl, listener);</pre>
 */
@Slf4j
public final class JooqTransactionManager extends BaseTransactionManager {

  private final ThreadLocal<Deque<DSLContext>> currentDsl = ThreadLocal.withInitial(LinkedList::new);

  /**
   * The parent jOOQ DSL context.
   */
  private final DSLContext parentDsl;

  /**
   * Creates the {@link org.jooq.TransactionListener} to wire into the {@link DSLContext}. See
   * class-level documentation for more detail.
   *
   * @param configuration The jOOQ configuration object to link into.
   * @return The transaction listener.
   */
  public static JooqTransactionListener createListener(DefaultConfiguration configuration) {
    var result = new JooqTransactionListener();
    configuration.set(result);
    return result;
  }

  /**
   * Creates the transaction manager.
   *
   * @param dslContext The DSL context.
   * @param listener The listener, linked to the DSL context.
   * @return The transaction manager.
   */
  public static JooqTransactionManager create(DSLContext dslContext, JooqTransactionListener listener) {
    var result = new JooqTransactionManager(dslContext);
    listener.setJooqTransactionManager(result);
    return result;
  }

  private JooqTransactionManager(DSLContext parentDsl) {
    this.parentDsl = parentDsl;
  }

  @Override
  public final <T> T inTransactionReturnsThrows(Callable<T> callable) {
    return transaction(connection -> callable.call());
  }

  private <T> T transaction(ThrowingFunction<Connection, T> work) {
    DSLContext dsl = currentDsl.get().isEmpty() ? parentDsl : currentDsl.get().peek();
    return dsl.transactionResult(
        config ->
            config
                .dsl()
                .connectionResult(
                    connection -> work.apply(connection)));
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
