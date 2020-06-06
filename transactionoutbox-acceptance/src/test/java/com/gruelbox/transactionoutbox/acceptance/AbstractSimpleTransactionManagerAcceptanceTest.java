package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.TransactionOutbox;
import com.gruelbox.transactionoutbox.jdbc.SimpleTransaction;
import com.gruelbox.transactionoutbox.jdbc.SimpleTransactionManager;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
abstract class AbstractSimpleTransactionManagerAcceptanceTest
    extends AbstractJdbcAcceptanceTest<SimpleTransaction<Void>, SimpleTransactionManager> {

  @Override
  protected final SimpleTransactionManager createTxManager() {
    return SimpleTransactionManager.fromConnectionDetails(
        connectionDetails().driverClassName(),
        connectionDetails().url(),
        connectionDetails().user(),
        connectionDetails().password());
  }

  @Override
  protected CompletableFuture<Void> scheduleWithTx(
      TransactionOutbox outbox, SimpleTransaction<Void> tx, int arg1, String arg2) {
    return outbox.schedule(AsyncInterfaceProcessor.class).processAsync(arg1, arg2, tx);
  }

  @Override
  protected CompletableFuture<Void> scheduleWithCtx(
      TransactionOutbox outbox, Object context, int arg1, String arg2) {
    throw new UnsupportedOperationException();
  }
}
