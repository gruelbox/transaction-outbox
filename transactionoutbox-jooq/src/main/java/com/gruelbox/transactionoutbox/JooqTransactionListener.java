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
    log.debug("Transaction start");
    jooqTransactionManager.pushLists();
  }

  @Override
  public void beginEnd(TransactionContext ctx) {
    ctx.dsl().connection(jooqTransactionManager::pushConnection);
    jooqTransactionManager.pushContext(ctx.dsl());
  }

  @Override
  public void commitStart(TransactionContext ctx) {
    log.debug("Transaction commit");
    try {
      jooqTransactionManager.flushBatches();
    } finally {
      jooqTransactionManager.closeBatchStatements();
    }
  }

  @Override
  public void commitEnd(TransactionContext ctx) {
    jooqTransactionManager.popConnection();
    jooqTransactionManager.popContext();
    try {
      jooqTransactionManager.processHooks();
    } finally {
      jooqTransactionManager.popLists();
    }
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
