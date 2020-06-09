package com.gruelbox.transactionoutbox.r2dbc;

import com.gruelbox.transactionoutbox.AlreadyScheduledException;
import com.gruelbox.transactionoutbox.PessimisticLockException;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.sql.Dialect;
import com.gruelbox.transactionoutbox.sql.SqlApi;
import com.gruelbox.transactionoutbox.sql.SqlStatement;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.R2dbcDataIntegrityViolationException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcTimeoutException;
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
  public Throwable mapSaveException(TransactionOutboxEntry entry, Throwable t) {
    try {
      throw underlying(t);
    } catch (R2dbcDataIntegrityViolationException e) {
      return new AlreadyScheduledException("Request " + entry.description() + " already exists", e);
    } catch (Throwable e) {
      return e;
    }
  }

  @Override
  public Throwable mapUpdateException(TransactionOutboxEntry entry, Throwable t) {
    try {
      throw underlying(t);
    } catch (R2dbcTimeoutException | TimeoutException | R2dbcNonTransientResourceException e) {
      return new PessimisticLockException(t);
    } catch (Throwable e) {
      if (e.getMessage().contains("timeout")) {
        return new PessimisticLockException(t);
      }
      return e;
    }
  }

  @Override
  public Throwable mapLockException(TransactionOutboxEntry entry, Throwable t) {
    try {
      throw underlying(t);
    } catch (R2dbcTimeoutException | TimeoutException | R2dbcNonTransientResourceException e) {
      return new PessimisticLockException(t);
    } catch (Throwable e) {
      if (e.getMessage().contains("timeout")) {
        return new PessimisticLockException(t);
      }
      return e;
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

  Throwable underlying(Throwable t) {
    if (t instanceof CompletionException) {
      return t.getCause();
    } else {
      return t;
    }
  }
}
