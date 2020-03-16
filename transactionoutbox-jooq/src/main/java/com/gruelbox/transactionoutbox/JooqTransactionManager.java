package com.gruelbox.transactionoutbox;

import java.sql.Connection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.impl.DefaultConfiguration;

/**
 * Transaction manager which uses jOOQ's transaction management.
 */
@Slf4j
public final class JooqTransactionManager extends BaseTransactionManager {

  private final ThreadLocal<Deque<DSLContext>> currentDsl = ThreadLocal.withInitial(LinkedList::new);

  /**
   * The parent jOOQ DSL context.
   */
  private final DSLContext parentDsl;

  public static JooqTransactionListener createListener(DefaultConfiguration configuration) {
    var result = new JooqTransactionListener();
    configuration.set(result);
    return result;
  }

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
