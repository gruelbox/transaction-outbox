package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.jdbc.AbstractThreadLocalJdbcTransactionManager;
import com.gruelbox.transactionoutbox.jdbc.SimpleTransaction;
import lombok.extern.slf4j.Slf4j;
import org.jooq.Configuration;
import org.jooq.DSLContext;

@Slf4j
final class ThreadLocalJooqTransactionManagerImpl
    extends AbstractThreadLocalJdbcTransactionManager<Configuration, JooqTransaction>
    implements ThreadLocalJooqTransactionManager {

  private final DSLContext parentDsl;

  ThreadLocalJooqTransactionManagerImpl(DSLContext parentDsl) {
    this.parentDsl = parentDsl;
  }

  @Override
  public <T, E extends Exception> T inTransactionReturnsThrows(
      ThrowingTransactionalSupplier<T, E, JooqTransaction> work) {
    DSLContext dsl =
        peekTransaction()
            .map(SimpleTransaction::context)
            .map(Configuration.class::cast)
            .map(Configuration::dsl)
            .orElse(parentDsl);
    return dsl.transactionResult(
        config ->
            config
                .dsl()
                .connectionResult(connection -> work.doWork(peekTransaction().orElseThrow())));
  }
}
