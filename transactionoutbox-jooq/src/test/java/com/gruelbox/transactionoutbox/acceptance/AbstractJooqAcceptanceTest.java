package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.JooqTransaction;
import com.gruelbox.transactionoutbox.JooqTransactionManager;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import java.util.concurrent.CompletableFuture;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

public abstract class AbstractJooqAcceptanceTest<TM extends JooqTransactionManager>
    extends AbstractJdbcAcceptanceTest<JooqTransaction, TM> {

  private DSLContext dsl;

  @Override
  protected CompletableFuture<Void> scheduleWithTx(
      TransactionOutbox outbox, JooqTransaction tx, int arg1, String arg2) {
    return outbox.schedule(AsyncInterfaceProcessor.class).processAsync(arg1, arg2, tx);
  }

  @Override
  protected CompletableFuture<Void> scheduleWithCtx(
      TransactionOutbox outbox, Object context, int arg1, String arg2) {
    return outbox
        .schedule(AsyncInterfaceProcessor.class)
        .processAsync(arg1, arg2, (Configuration) context);
  }

  @Override
  protected CompletableFuture<?> runSql(Object txOrContext, String sql) {
    Configuration configuration;
    if (txOrContext instanceof JooqTransaction) {
      configuration = ((JooqTransaction) txOrContext).context();
    } else {
      configuration = (Configuration) txOrContext;
    }
    return DSL.using(configuration).query(sql).executeAsync().toCompletableFuture();
  }

  @Override
  protected CompletableFuture<Long> readLongValue(JooqTransaction jooqTransaction, String sql) {
    return DSL.using(jooqTransaction.context())
        .resultQuery(sql)
        .fetchAsync()
        .thenApply(result -> (Long) result.getValue(0, 0))
        .toCompletableFuture();
  }
}
