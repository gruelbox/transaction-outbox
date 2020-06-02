package com.gruelbox.transactionoutbox;

import lombok.extern.slf4j.Slf4j;
import org.jooq.Configuration;
import org.jooq.DSLContext;

@Slf4j
@Beta
final class DefaultJooqTransactionManagerImpl implements DefaultJooqTransactionManager {

  private final DSLContext dsl;

  DefaultJooqTransactionManagerImpl(DSLContext dsl) {
    this.dsl = dsl;
  }

  @Override
  public <T, E extends Exception> T inTransactionReturnsThrows(
      ThrowingTransactionalSupplier<T, E, JooqTransaction> work) {
    return dsl.transactionResult(cfg -> work.doWork(transactionFromContext(cfg)));
  }

  @Override
  public JooqTransaction transactionFromContext(Configuration context) {
    Object txn = context.data(JooqTransactionListener.TXN_KEY);
    if (txn == null) {
      throw new IllegalStateException(
          JooqTransactionListener.class.getSimpleName() + " is not attached to the DSL");
    }
    return (JooqTransaction) txn;
  }

  @Override
  public Class<Configuration> contextType() {
    return Configuration.class;
  }
}
