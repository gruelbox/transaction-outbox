package com.gruelbox.transactionoutbox;

import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;

/**
 * jOOQ transaction manager which uses explicitly-passed transaction context. Suitable for use with
 * {@link org.jooq.impl.DefaultTransactionProvider}.
 */
@Slf4j
final class DefaultJooqTransactionManager implements JooqTransactionManager {

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
  public <T, E extends Exception> T requireTransactionReturns(
      ThrowingTransactionalSupplier<T, E> work) {
    throw new UnsupportedOperationException();
  }
}
