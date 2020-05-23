package com.gruelbox.transactionoutbox.r2dbc;

import com.gruelbox.transactionoutbox.AlreadyScheduledException;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.Utils;
import com.gruelbox.transactionoutbox.sql.Dialect;
import com.gruelbox.transactionoutbox.sql.SqlApi;
import com.gruelbox.transactionoutbox.sql.SqlStatement;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.R2dbcDataIntegrityViolationException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcTimeoutException;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class R2dbcSqlApi implements SqlApi<Connection, R2dbcTransaction> {

  @Override
  public boolean requiresNativeStatementMapping() {
    return true;
  }

  @Override
  public void handleSaveException(TransactionOutboxEntry entry, Throwable t) {
    try {
      if (t instanceof CompletionException) {
        throw t.getCause();
      } else {
        throw t;
      }
    } catch (R2dbcDataIntegrityViolationException e) {
      throw new AlreadyScheduledException("Request " + entry.description() + " already exists", e);
    } catch (Throwable e) {
      throw (RuntimeException) Utils.uncheckAndThrow(e);
    }
  }

  @Override
  public List<Integer> handleLockException(TransactionOutboxEntry entry, Throwable t)
      throws Throwable {
    // TODO inconsistencies of drivers - report to R2DBC
    if (t instanceof R2dbcTimeoutException
        || t instanceof TimeoutException
        || ((t instanceof R2dbcNonTransientResourceException)
            && t.getMessage().contains("timeout"))) {
      return List.of();
    } else {
      throw t;
    }
  }

  @Override
  public <T> T statement(
      R2dbcTransaction tx,
      Dialect dialect,
      String sql,
      int timeoutSeconds,
      boolean batchable,
      Function<SqlStatement, T> binding) {
    return binding.apply(new R2dbcStatement(tx, dialect, timeoutSeconds, sql));
  }
}
