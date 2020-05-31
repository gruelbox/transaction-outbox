package com.gruelbox.transactionoutbox.r2dbc;

import com.gruelbox.transactionoutbox.AlreadyScheduledException;
import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.SqlPersistor.Binder;
import com.gruelbox.transactionoutbox.SqlPersistor.Handler;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.Utils;
import com.gruelbox.transactionoutbox.jdbc.JdbcPersistor;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.R2dbcDataIntegrityViolationException;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletionException;
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
  public void interceptNew(Dialect dialect, TransactionOutboxEntry entry) {
    // TODO remove hack https://github.com/mirromutth/r2dbc-mysql/issues/111
    if (dialect.equals(Dialect.MY_SQL_5) || dialect.equals(Dialect.MY_SQL_8)) {
      entry.setNextAttemptTime(entry.getNextAttemptTime().truncatedTo(ChronoUnit.SECONDS));
    }
  }

  @Override
  public String preprocessSql(Dialect dialect, String sql) {
    if (!dialect.equals(Dialect.POSTGRESQL_9)) {
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
      boolean batchable,
      Function<Binder, T> binding) {
    return binding.apply(new R2dbcStatement(tx, dialect, sql));
  }
}
