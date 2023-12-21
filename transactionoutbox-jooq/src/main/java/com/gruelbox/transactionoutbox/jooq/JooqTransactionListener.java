package com.gruelbox.transactionoutbox.jooq;

import com.gruelbox.transactionoutbox.spi.SimpleTransaction;
import lombok.extern.slf4j.Slf4j;
import org.jooq.TransactionContext;
import org.jooq.TransactionListener;

/** A jOOQ {@link TransactionListener} which synchronises a {@link JooqTransactionManager}. */
@Slf4j
public class JooqTransactionListener implements TransactionListener {

  static final String TXN_KEY = JooqTransactionListener.class.getName() + ".txn";

  private ThreadLocalJooqTransactionManager jooqTransactionManager;

  protected JooqTransactionListener() {}

  void setJooqTransactionManager(ThreadLocalJooqTransactionManager jooqTransactionManager) {
    this.jooqTransactionManager = jooqTransactionManager;
  }

  @Override
  public void beginStart(TransactionContext ctx) {
    // No-op
  }

  @Override
  public void beginEnd(TransactionContext ctx) {
    ctx.dsl()
        .connection(
            connection -> {
              SimpleTransaction transaction =
                  new SimpleTransaction(connection, ctx.dsl().configuration());
              ctx.dsl().configuration().data(TXN_KEY, transaction);
              if (jooqTransactionManager != null) {
                jooqTransactionManager.pushTransaction(transaction);
              }
            });
  }

  @Override
  public void commitStart(TransactionContext ctx) {
    log.debug("Transaction commit start");
    try {
      getTransaction(ctx).flushBatches();
    } finally {
      getTransaction(ctx).close();
    }
  }

  @Override
  public void commitEnd(TransactionContext ctx) {
    log.debug("Transaction commit end");
    SimpleTransaction transaction = getTransaction(ctx);
    safePopThreadLocals();
    transaction.processHooks();
  }

  @Override
  public void rollbackStart(TransactionContext ctx) {
    log.debug("Transaction rollback");
    getTransaction(ctx).close();
  }

  @Override
  public void rollbackEnd(TransactionContext ctx) {
    safePopThreadLocals();
  }

  private SimpleTransaction getTransaction(TransactionContext ctx) {
    return (SimpleTransaction) ctx.dsl().configuration().data(TXN_KEY);
  }

  private void safePopThreadLocals() {
    if (jooqTransactionManager != null) {
      jooqTransactionManager.popTransaction();
    }
  }
}
