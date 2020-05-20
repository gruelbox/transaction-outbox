package com.gruelbox.transactionoutbox;

import lombok.extern.slf4j.Slf4j;
import org.jooq.Configuration;
import org.jooq.DSLContext;

/**
 * jOOQ transaction manager which uses explicitly-passed transaction context. Suitable for use with
 * {@link org.jooq.impl.DefaultTransactionProvider}. Relies on {@link JooqTransactionListener} being
 * connected to the {@link DSLContext}.
 */
@Slf4j
final class DefaultJooqTransactionManager
    implements ParameterContextTransactionManager<Configuration> {

  private final DSLContext dsl;

  DefaultJooqTransactionManager(DSLContext dsl) {
    this.dsl = dsl;
  }

  @Override
  public <T, E extends Exception> T inTransactionReturnsThrows(
      ThrowingTransactionalSupplier<T, E> work) {
    return dsl.transactionResult(cfg -> work.doWork(transactionFromContext(cfg)));
  }

  @Override
  public Transaction transactionFromContext(Configuration context) {
    Object txn = context.data(JooqTransactionListener.TXN_KEY);
    if (txn == null) {
      throw new IllegalStateException(
          JooqTransactionListener.class.getSimpleName() + " is not attached to the DSL");
    }
    return (Transaction) txn;
  }

  @Override
  public Class<Configuration> contextType() {
    return Configuration.class;
  }
}
