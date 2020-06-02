package com.gruelbox.transactionoutbox.sql;

import com.gruelbox.transactionoutbox.Beta;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.spi.Transaction;
import java.util.List;
import java.util.function.Function;

@Beta
public interface SqlApi<CN, TX extends Transaction<CN, ?>> {

  boolean requiresNativeStatementMapping();

  <T> T statement(
      TX tx,
      Dialect dialect,
      String sql,
      int writeLockTimeoutSeconds,
      boolean batchable,
      Function<SqlStatement, T> binding);

  void handleSaveException(TransactionOutboxEntry entry, Throwable t);

  List<Integer> handleLockException(TransactionOutboxEntry entry, Throwable t) throws Throwable;
}
