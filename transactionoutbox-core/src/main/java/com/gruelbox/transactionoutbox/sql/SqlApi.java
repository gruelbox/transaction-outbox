package com.gruelbox.transactionoutbox.sql;

import com.gruelbox.transactionoutbox.Beta;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.spi.BaseTransaction;
import java.util.function.Function;

@Beta
public interface SqlApi<CN, TX extends BaseTransaction<CN>> {

  boolean requiresNativeStatementMapping();

  <T> T statement(
      TX tx,
      Dialect dialect,
      String sql,
      int writeLockTimeoutSeconds,
      boolean batchable,
      Function<SqlStatement, T> binding);

  Throwable mapSaveException(TransactionOutboxEntry entry, Throwable t);

  Throwable mapLockException(TransactionOutboxEntry entry, Throwable t);

  Throwable mapUpdateException(TransactionOutboxEntry entry, Throwable e);
}
