package com.gruelbox.transactionoutbox;

import org.jooq.Configuration;
import org.jooq.DSLContext;

/**
 * @deprecated use {@link com.gruelbox.transactionoutbox.jooq.JooqTransactionManager}
 */
@Deprecated(forRemoval = true)
public interface JooqTransactionManager extends TransactionManager {

  static JooqTransactionListener createListener() {
    return new com.gruelbox.transactionoutbox.JooqTransactionListener();
  }

  static ThreadLocalContextTransactionManager create(
      DSLContext dslContext, JooqTransactionListener listener) {
    return com.gruelbox.transactionoutbox.jooq.JooqTransactionManager.create(dslContext, listener);
  }

  static ParameterContextTransactionManager<Configuration> create(DSLContext dslContext) {
    return com.gruelbox.transactionoutbox.jooq.JooqTransactionManager.create(dslContext);
  }
}
