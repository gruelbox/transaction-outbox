package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.spi.BaseTransaction;
import com.gruelbox.transactionoutbox.spi.BaseTransactionManager;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractSqlAcceptanceTest<
        CN, TX extends BaseTransaction<CN>, TM extends BaseTransactionManager<CN, ? extends TX>>
    extends AbstractAcceptanceTest<CN, TX, TM> {

  private final AtomicInteger next = new AtomicInteger();

  protected abstract CompletableFuture<?> runSql(Object txOrContext, String sql);

  protected abstract CompletableFuture<Long> readLongValue(TX tx, String sql);

  protected final void prepareDataStore() {
    txManager
        .transactionally(tx -> runSql(tx, "CREATE TABLE IF NOT EXISTS TESTDATA(value INT)"))
        .thenRun(() -> log.info("Table created"))
        .join();
  }

  @Override
  protected final void cleanDataStore() {
    txManager
        .transactionally(
            tx ->
                runSql(tx, "DELETE FROM TXNO_OUTBOX")
                    .thenCompose(__ -> runSql(tx, "DELETE FROM TESTDATA")))
        .thenRun(() -> log.info("Database cleaned"))
        .join();
  }

  @Override
  protected final CompletableFuture<?> incrementRecordCount(Object txOrContext) {
    return runSql(txOrContext, "INSERT INTO TESTDATA VALUES(" + next.incrementAndGet() + ")");
  }

  @Override
  protected final CompletableFuture<Long> countRecords(TX tx) {
    return readLongValue(tx, "SELECT COUNT(*) FROM TESTDATA");
  }
}
