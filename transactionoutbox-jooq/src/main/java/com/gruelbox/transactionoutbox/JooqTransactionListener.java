package com.gruelbox.transactionoutbox;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jooq.TransactionContext;
import org.jooq.TransactionListener;

@Slf4j
public class JooqTransactionListener implements TransactionListener {

  @Setter
  private JooqTransactionManager jooqTransactionManager;

  JooqTransactionListener() {}

  @Override
  public void beginStart(TransactionContext ctx) {
    // No-op
  }

  @Override
  public void beginEnd(TransactionContext ctx) {
    log.debug("Transaction start");
    jooqTransactionManager.pushLists();
    ctx.dsl().connection(jooqTransactionManager::pushConnection);
    jooqTransactionManager.pushContext(ctx.dsl());
  }

  @Override
  public void commitStart(TransactionContext ctx) {
    log.debug("Transaction commit");
    jooqTransactionManager.flushBatches();
    jooqTransactionManager.closeBatchStatements();
  }

  @Override
  public void commitEnd(TransactionContext ctx) {
    jooqTransactionManager.popConnection();
    jooqTransactionManager.popContext();
    jooqTransactionManager.processHooks();
  }

  @Override
  public void rollbackStart(TransactionContext ctx) {
    log.debug("Transaction rollback");
    jooqTransactionManager.closeBatchStatements();
  }

  @Override
  public void rollbackEnd(TransactionContext ctx) {
    commitEnd(ctx);
  }
}
