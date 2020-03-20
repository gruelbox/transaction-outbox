package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.AbstractThreadLocalTransactionManager.ThreadLocalTransaction;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jooq.TransactionContext;
import org.jooq.TransactionListener;

/** A jOOQ {@link TransactionListener} which synchronises a {@link JooqTransactionManager}. */
@Slf4j
public class JooqTransactionListener implements TransactionListener {

  @Setter private JooqTransactionManager jooqTransactionManager;

  JooqTransactionListener() {}

  @Override
  public void beginStart(TransactionContext ctx) {
    // No-op
  }

  @Override
  public void beginEnd(TransactionContext ctx) {
    ctx.dsl()
        .connection(
            connection ->
                jooqTransactionManager.pushTransaction(new ThreadLocalTransaction(connection)));
    jooqTransactionManager.pushContext(ctx.dsl());
  }

  @Override
  public void commitStart(TransactionContext ctx) {
    log.debug("Transaction commit start");
    try {
      jooqTransactionManager.peekTransaction().orElseThrow().flushBatches();
    } finally {
      jooqTransactionManager.peekTransaction().orElseThrow().close();
    }
  }

  @Override
  public void commitEnd(TransactionContext ctx) {
    log.debug("Transaction commit end");
    ThreadLocalTransaction transaction = jooqTransactionManager.popTransaction();
    jooqTransactionManager.popContext();
    transaction.processHooks();
  }

  @Override
  public void rollbackStart(TransactionContext ctx) {
    log.debug("Transaction rollback");
    jooqTransactionManager.peekTransaction().orElseThrow().close();
  }

  @Override
  public void rollbackEnd(TransactionContext ctx) {
    ThreadLocalTransaction transaction = jooqTransactionManager.popTransaction();
    jooqTransactionManager.popContext();
  }
}
