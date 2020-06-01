package com.gruelbox.transactionoutbox.r2dbc;

import static com.gruelbox.transactionoutbox.DialectFamily.MY_SQL;
import static com.gruelbox.transactionoutbox.DialectFamily.POSTGRESQL;

import com.gruelbox.transactionoutbox.AlreadyScheduledException;
import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.SqlPersistor.Binder;
import com.gruelbox.transactionoutbox.SqlPersistor.Handler;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.Utils;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.R2dbcDataIntegrityViolationException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcTimeoutException;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class R2dbcSqlHandler implements Handler<Connection, R2dbcTransaction<?>> {

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
  public void interceptNew(Dialect dialect, TransactionOutboxEntry entry) {
    // TODO remove hack https://github.com/mirromutth/r2dbc-mysql/issues/111
    if (dialect.getFamily().equals(MY_SQL)) {
      entry.setNextAttemptTime(entry.getNextAttemptTime().truncatedTo(ChronoUnit.SECONDS));
    }
  }

  @Override
  public String preprocessSql(Dialect dialect, String sql) {
    // TODO delegate to the dialect
    if (!dialect.getFamily().equals(POSTGRESQL)) {
      return sql;
    }
    // Blunt, not general purpose, but only needs to work for the known use cases
    StringBuilder builder = new StringBuilder();
    int paramIndex = 1;
    for (int i = 0; i < sql.length(); i++) {
      char c = sql.charAt(i);
      if (c == '?') {
        builder.append('$').append(paramIndex++);
      } else {
        builder.append(c);
      }
    }
    return builder.toString();
  }

  @Override
  public <T> T statement(
      R2dbcTransaction<?> tx,
      Dialect dialect,
      String sql,
      int timeoutSeconds,
      boolean batchable,
      Function<Binder, T> binding) {
    return binding.apply(new R2dbcStatement(tx, dialect, timeoutSeconds, sql));
  }
}
